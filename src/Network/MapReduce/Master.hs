{-# LANGUAGE OverloadedStrings #-}

{-|
Module: Network.MapReduce.Master
Use this module to create an IO action that runs on the master server
The main function: startMasterWith
-}

-- The contract between the master and the worker:
--  after accepting connection:
--    Server now knows worker and start scheduling 

module Network.MapReduce.Master 
(
  startMasterWith
)
  
where


import Network.WebSockets
import Network.MapReduce.Types
import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception
import Data.UUID.V4
import Data.UUID (toString)
import Data.Aeson
import Data.ByteString.Lazy (ByteString)
import Data.Maybe
import Data.List (transpose)

uniqueID :: IO String
uniqueID = fmap toString nextRandom

data Job = Job {
         jobID :: String                     -- ^ unique identifier
       , jobWorkerChan :: Chan Worker        -- ^ idle workers will be placed here
       , jobReducerCount :: [Int]            -- ^ number of reducers of next stages, implies the number of stages
       , jobInputs :: [[String]]             -- ^ inputs into the current stage
       , jobStageID :: Int                   -- ^ current stage id
       }

data Stage = Stage {
          workerChan :: Chan Worker          -- ^ idle workers
        , stageID :: Int                     -- ^ stage id
        , stageInputs :: [[String]]          -- ^ inputs into the stage
        , stageReducerCount :: Int           
        }

-- | if no stages are left return Nothing
currentStage :: Job -> Maybe Stage
currentStage (Job _ wc rc input sid) = fmap (Stage wc sid input) (listToMaybe rc)

       
-- | fetch one idle worker and send it the cmd
-- restart if anything goes wrong in between
-- put back the worker if success
cmdWorker :: Chan Worker           -- ^ chan of idle worker 
          -> WorkerCmd
          -> IO [String]           -- ^ outputs
cmdWorker wc workerCmd = do
    w <- readChan wc
    let conn = wconn w
    m <- catch (send conn (DataMessage (Text (encode workerCmd))) >> fmap Just (receiveData conn))
               ((\_ -> return Nothing) :: ConnectionException -> IO (Maybe ByteString))
    let r = m >>= decode
    -- if failed, restart the work
    -- otherwise put worker back into the idle pool and return the value
    maybe (shutDownWorker w >> cmdWorker wc workerCmd) (\val -> writeChan wc w >> return val) r

-- | run workers in parallel, returns a list of partitioned output
runStage :: Stage -> IO [[String]]
runStage (Stage wc i inputs rc) = transpose <$>
    mapConcurrently (\(input, wid) -> cmdWorker wc (WorkerCmd wid i input rc))
                    (zip inputs [0..])

-- | returns a new job with a unique id
newJob :: [[String]]    -- ^ inputs, implies the number of reducers at initial stage
       -> [Int]         -- ^ num of reducers of each stage
       -> IO Job
newJob inputs reducers = do
    uuid <- uniqueID
    c <- newChan
    return $ Job uuid c reducers inputs 0


runJob :: Job -> IO [[String]]
runJob j@(Job jid wc rc inputs sid)
  | null rc = return inputs
  | otherwise = do
      next <- runStage (fromJust $ currentStage j) -- XXX: avoid fromJust
      runJob (Job jid wc (tail rc) next (sid + 1))


data Worker = Worker {
            wconn :: Connection              -- ^ ws connection
          , closeVar :: MVar ()              -- ^ signal for close
            }

shutDownWorker :: Worker -> IO ()
shutDownWorker (Worker _ c) = putMVar c ()

newWorker :: Connection -> IO Worker
newWorker c = do
    m <- newEmptyMVar
    return (Worker c m)


-- | the IO action for the ws server
serve :: Chan Worker -> PendingConnection ->  IO ()
serve wc p = do
    c <- acceptRequest p
    w <- newWorker c
    writeChan wc w
    takeMVar (closeVar w)
    sendClose c ("" :: ByteString)

startMasterWith :: [[String]]         -- ^ first batch of input
                -> [Int]              -- ^ number of reducers 
                -> String             -- ^ Host
                -> Int                -- ^ port to listen on
                -> IO [[String]]
startMasterWith input rc host p = do
    job <- newJob input rc {- XXX: maybe move this logic out of this file? -}
    wsserver <- forkIO $ runServer host p (serve (jobWorkerChan job))
    out <- runJob job
    killThread wsserver
    return out

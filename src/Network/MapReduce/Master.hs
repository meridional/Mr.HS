{-# LANGUAGE OverloadedStrings #-}

-- The contract between the master and the worker:
--  after accepting connection:
--    Worker send: Ping _
--    Server send: Pong "ack"
--    Server now knows worker and start scheduling 

module Network.MapReduce.Master where
  -- XXX: control output list

import Network.WebSockets
import Network.MapReduce.Types
import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception
import Data.UUID.V4
import Data.UUID (toString)
import Data.Aeson
import Data.ByteString.Lazy (ByteString)
import Data.Maybe

uniqueID :: IO String
uniqueID = fmap toString nextRandom

data Job = Job {
         jobID :: String                     -- ^ unique identifier
       , jobWorkerChan :: Chan Connection    -- ^ idle workers will be placed here
       , jobReducerCount :: [Int]            -- ^ number of reducers of next stages, implies the number of stages
       , jobInputs :: [[String]]             -- ^ inputs into the current stage
       , jobStageID :: Int                   -- ^ current stage id
       }

data Stage = Stage {
          workerChan :: Chan Connection      -- ^ idle workers
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
cmdWorker :: Chan Connection       -- ^ chan of idle worker 
          -> WorkerCmd
          -> IO [String]           -- ^ outputs
cmdWorker wc workerCmd = do
    w <- readChan wc
    sendDataMessage w (Text (encode workerCmd))
    m <- catch (fmap Just (receiveData w)) 
               ((\_ -> return Nothing) :: ConnectionException -> IO (Maybe ByteString))
    let r = m >>= decode
    -- if failed, restart the work
    -- otherwise put worker back into the idle pool
    maybe (cmdWorker wc workerCmd) (\val -> writeChan wc w >> return val) r

-- | run workers in parallel
runStage :: Stage -> IO [[String]]
runStage (Stage wc i inputs rc) = 
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


{-
echo :: Connection -> IO ()
echo conn = do
    putStrLn "serve"
    m <- receive conn
    case m of (ControlMessage (Close _ _)) -> send conn (ControlMessage (Close (fromIntegral (0 :: Int)) ""))
              (ControlMessage (Ping "Worker")) -> send conn (ControlMessage (Pong "hello, worker")) >> echo conn
              (ControlMessage (Ping _)) -> send conn (ControlMessage (Pong "hello")) >> echo conn
              (ControlMessage _) -> echo conn
              _ -> send conn m >> echo conn
-}

-- | register a worker and send it into the worker chan 
register :: Connection -> Chan Connection -> IO ()
register conn wc = do
    m <- receive conn
    case m of (ControlMessage (Ping _)) -> writeChan wc conn
              _ -> send conn (ControlMessage (Close (fromIntegral (0 :: Int)) ""))

-- | the IO action for the ws server
serve :: Chan Connection -> PendingConnection ->  IO ()
serve wc p = do
    let path = requestPath (pendingRequest p)
    print path
    if path == "/" then acceptRequest p >>= \c -> register c wc
                   else rejectRequest p "wrong path" 

startMasterWith :: [[String]]         -- ^ first batch of input
                -> [Int]              -- ^ number of reducers 
                -> Int                -- ^ port to listen on
                -> IO [[String]]
startMasterWith input rc p = do
    job <- newJob input rc {- XXX: maybe move this logic out of this file? -}
    wsserver <- forkIO $ runServer "127.0.0.1" p (serve (jobWorkerChan job))
    out <- runJob job
    killThread wsserver
    return out


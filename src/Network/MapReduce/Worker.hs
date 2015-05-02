{-# LANGUAGE OverloadedStrings #-}

module Network.Mapreduce.Worker where

-- TODO: restrict export 

import Network.MapReduce.Types
import Network.WebSockets
import Data.Aeson
{-import Data.Maybe-}
import Control.Monad


type StageFunction = Int           -- ^ worker id
                   -> Int          -- ^ num of partions of output
                   -> [String]     -- ^ inputs
                   -> IO [String]

type WorkerFunctions = [StageFunction]

extractWorkCmd :: DataMessage -> Maybe WorkerCmd
extractWorkCmd (Binary _) = Nothing
extractWorkCmd (Text t) = decode t

phoneHome :: Connection -> [String] -> IO ()
phoneHome conn result = sendDataMessage conn (Text (encode result))

executeCmd :: WorkerFunctions -> WorkerCmd -> IO [String]
executeCmd wfs (WorkerCmd wid sid input rc) =
    (wfs !! sid) wid rc input

work :: WorkerFunctions -> Connection -> IO ()
work wfs master = forever $ do
    f <- fmap extractWorkCmd (receiveDataMessage master)
    maybe (return ()) (executeCmd wfs >=> phoneHome master) f
    
startWorkerWith :: WorkerFunctions       
           -> String                  -- ^ master's addr
           -> Int                     -- ^ master's port
           -> IO ()
startWorkerWith wfs host port = runClient host port "/" (work wfs)

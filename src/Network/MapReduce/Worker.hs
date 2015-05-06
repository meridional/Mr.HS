{-# LANGUAGE OverloadedStrings #-}

{-|
Module: Network.MapReduce.Worker
Use this module to create an IO action that runs on the worker server
The main function: startWorkerWith
-}

module Network.MapReduce.Worker 
(
  startWorkerWith
, StageFunction
, WorkerFunctions
)

where


import Network.MapReduce.Types
import Network.WebSockets
import Data.Aeson
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
    m <- receiveDataMessage master
    let f = extractWorkCmd m
    maybe (return ()) (executeCmd wfs >=> phoneHome master) f
    
-- | takes a list of worker functions 
-- master's address info
-- and returns an io action that communicates with
-- the master and runs the correct worker function
startWorkerWith :: WorkerFunctions       
           -> String                  -- ^ master's addr
           -> Int                     -- ^ master's port
           -> IO ()
startWorkerWith wfs host port = runClient host port "/" (work wfs)

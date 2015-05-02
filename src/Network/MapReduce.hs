{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Trustworthy #-}
-- XXX: this is just a placeholder file
module Network.MapReduce 

where

import qualified Data.ByteString.Lazy as BL
import System.Environment
import Network
import qualified Data.Set as S
import Control.Concurrent.MVar
import Control.Concurrent.Chan
import Control.Concurrent
import System.IO
import Control.Exception

type Action = (Int, (BL.ByteString, [BL.ByteString]) -> [(BL.ByteString, BL.ByteString)])

printUsage :: IO ()
printUsage = do
    print "hello"

mapreduceWith :: [Action] -> [FilePath] -> IO ()
mapreduceWith acts fs = do
    x <- fmap (fromIntegral . read . head) getArgs
    s <- listenOn (PortNumber x)
    undefined

runActionOne :: [FilePath] -> Chan PortID -> IO ([[FilePath]])
runActionOne = undefined


-- | (job id, file paths for each reducer of the next stage)
type JobOutput = (Int, [FilePath])

-- | startwork, helper method, returns immediately
startWork :: Int -> [FilePath] -> Handle -> IO ()
startWork stageid inputs wh = do
    hPutStrLn wh (show stageid)
    hPutStrLn wh (show inputs)

-- | runWorker :: stage id, job id, Channel of worker handles, channel of returning output
runWorker :: Int              -- stage id
          -> Int              -- job id
          -> [FilePath]       -- input files
          -> Chan Handle      -- chan of workers
          -> Chan JobOutput   -- chan to return output
          -> IO ()
runWorker sid jid inputs wc outc = do
    wh <- readChan wc
    startWork sid inputs wh
    onException (do
                s <- hGetLine wh
                writeChan wc wh
                writeChan outc undefined
                )
                (do
                hClose wh
                runWorker sid jid inputs wc outc)


-- | runAction - takes stage id, file paths for each reducer and a channel
--    supplying worker handles and returns a new set of paths for the next
--    stage
runAction :: Int -> [[FilePath]] -> Chan Handle -> IO ([[FilePath]])
runAction = undefined

runMaster :: [Action] -> [FilePath] -> PortNumber -> IO ()
runMaster acts fs port = do
    workersRef <- newChan 
    currentStage <- newMVar 0
    s <- listenOn (PortNumber port)
    undefined


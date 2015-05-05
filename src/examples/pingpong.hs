module Main where
import Network.MapReduce.Master
import Network.MapReduce.Worker
import Control.Concurrent
import Text.Printf
import Control.Monad

port :: Int
port = 8080

host :: String
host = "127.0.0.1"

testFunction :: StageFunction
testFunction wid rc input = do
    printf "%d %d\n" wid rc
    print input
    return $ map (\x -> printf "%d" x ++ show input) [1..rc]
    

master :: IO [[String]]
master = startMasterWith [["hello", "world"], ["world", "hello"]] [20, 30, 40] host port

main :: IO ()
main = do
    replicateM_ 4 (void $ forkIO $ threadDelay 1000000 >> void (startWorkerWith [testFunction, testFunction, testFunction] host port))
    master >>= print 
    return ()

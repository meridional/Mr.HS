{-# LANGUAGE OverloadedStrings #-}
import Network.MapReduce.Classic
import Text.Printf
import System.Environment
import Control.Monad
import Control.Concurrent

mapper :: MapperFun String Int
mapper _ v = map (\x -> (x, 1)) (words v)

reducer :: ReducerFun String Int
reducer = unlines . map (\(k, l) -> printf "%s %d" k (length l)) 

printUsage :: IO ()
printUsage = putStrLn "./wc [files]"

main :: IO ()
main = do
    s <- getArgs
    let (master, worker) = mapreduceWith
          (mapper, const 1, reducer) s 1 "127.0.0.1" 8080
    if null s
      then printUsage
      else replicateM_ (length s) (forkIO worker) >> master >>= readFile . head
        >>= putStrLn

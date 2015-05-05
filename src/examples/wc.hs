{-# LANGUAGE OverloadedStrings #-}
import Network.MapReduce.Classic
import System.Environment
import Control.Monad
import Control.Concurrent
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.ByteString.Builder
import Data.Monoid

mapper :: MapperFun BL.ByteString Int
mapper _ v = map (\x -> (x, 1)) (BL.words v)

reducer :: ReducerFun BL.ByteString Int
reducer = BL.unlines . map (\(k, l) -> toLazyByteString $ lazyByteString k <> char8 ' ' <> intDec (length l))

printUsage :: IO ()
printUsage = putStrLn "./wc [files]"

main :: IO ()
main = do
    s <- getArgs
    let (master, worker) = mapreduceWith
          (mapper, const 1, reducer) s 1 "127.0.0.1" 8080
    if null s
      then printUsage
      else replicateM_ 4 {- fork 4 workers -} (forkIO (threadDelay 1000 >> worker)) >> master >>= BL.readFile . head
        >>= BL.putStrLn

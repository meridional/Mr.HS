{-# LANGUAGE ScopedTypeVariables #-}
module Network.MapReduce.Classic 
(
  mapreduceWith
, MapperFun
, HashFun
, ReducerFun
, MRFun
)

where

import Network.MapReduce
import Data.Binary
import Data.UUID.V4
import Data.UUID
import Control.Monad
import qualified Data.Map as M
import Control.Applicative ((<$>))
import Control.Concurrent
import Data.ByteString.Lazy.Char8 (ByteString)
import qualified Data.ByteString.Lazy.Char8 as BL

type MapperFun k v = String            -- ^ file name
                   -> ByteString           -- ^ contents of the file
                   -> [(k, v)]         -- ^ output

type HashFun k = k -> Int

type ReducerFun k v = [(k, [v])]
                    -> ByteString           -- ^ arbitrary string

type MRFun k v = (MapperFun k v, HashFun k, ReducerFun k v)

-- | Output file of the mapper is guranteed to be sorted and partitioned
-- according to the hash function, each reducer still need to merge the
-- sorted partitions
mapperToStageFun :: (Binary k, Ord k, Binary v) => MapperFun k v -> HashFun k -> StageFunction
mapperToStageFun _ _ _ _ [] = error "empty input"
mapperToStageFun mf hf _ parts (input:_) = do
    contents <- BL.readFile input
    let r = foldr (\(k,v) acc -> M.insertWith (++) k [v] acc) M.empty (mf input contents)
        empty = M.fromList $ zip [0..(parts-1)] (repeat []) 
        r' = M.toList $  M.foldrWithKey (\k vl acc -> M.insertWith (++) (hf k `mod` parts) [(k, vl)] acc) empty r
    forM r' (\((_, kvs) :: (Int, [(k, [v])])) -> do
      fname <- fmap toString nextRandom
      encodeFile fname kvs 
      return fname)

reducerToStageFun :: (Binary k, Ord k, Binary v) => ReducerFun k v -> StageFunction
reducerToStageFun rf _ _ inputs = do
    contents <- fmap (M.toList . foldr (uncurry (M.insertWith (++))) M.empty  .  concat) (mapM decodeFile inputs)
    let x = rf contents
    fname <- fmap toString nextRandom
    BL.writeFile fname x
    return [fname]

mapreduceWorkerWith :: (Binary k, Ord k, Binary v) =>
                       MRFun k v          -- ^ mapper fun, hash fun and reducer fun
                    -> String             -- ^ master's address
                    -> Int                -- ^ master's port number
                    -> IO ()
mapreduceWorkerWith (m, h, r) = 
    startWorkerWith [mapperToStageFun m h, reducerToStageFun r] 

mapreduceWith :: (Binary k, Ord k, Binary v) =>
                 MRFun k v                -- ^ mapper fun, hash fun and reducer fun
              -> [String]                 -- ^ input to the mapper
              -> Int                      -- ^ num of reducer
              -> String                   -- ^ host
              -> Int                      -- ^ port
              -> (IO [String], IO ())     -- ^ (master computation , worker computation)
mapreduceWith mhr inputs partitions host port =
    (head <$> startMasterWith (map (:[]) inputs) [partitions, 1] host port,
     threadDelay 1000 >> mapreduceWorkerWith mhr host port)

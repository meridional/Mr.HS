{-# LANGUAGE OverloadedStrings #-}
module Network.MapReduce.Types where


import Data.Aeson
import Control.Applicative
import Control.Monad


-- | Defines the data to pass to the worker 
data WorkerCmd = WorkerCmd {
          wcWorkerID :: Int
        , wcStageID :: Int
        , wcInput :: [String]
        , wcReducerCount :: Int
        } deriving Show

instance FromJSON WorkerCmd where
    parseJSON (Object v) = WorkerCmd <$>
                           v .: "wid" <*>
                           v .: "sid" <*>
                           v .: "inputs" <*>
                           v .: "rc" 
    parseJSON _ = mzero

instance ToJSON WorkerCmd where
    toJSON (WorkerCmd wid sid inputs rc) = object ["wid" .= show wid,
                                                   "sid" .= show sid,
                                                   "inputs" .= inputs,
                                                   "rc" .= rc]


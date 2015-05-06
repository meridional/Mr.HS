{-# LANGUAGE OverloadedStrings, DeriveGeneric #-}

{-|
Module: Network.MapReduce.Types
The types that is shared by Master and Worker are defined here
-}
module Network.MapReduce.Types where


import Data.Aeson
import GHC.Generics


-- | Defines the data to pass to the worker 
data WorkerCmd = WorkerCmd {
          wcWorkerID :: Int
        , wcStageID :: Int
        , wcInput :: [String]
        , wcReducerCount :: Int
        } deriving (Show, Generic)

instance FromJSON WorkerCmd
instance ToJSON WorkerCmd


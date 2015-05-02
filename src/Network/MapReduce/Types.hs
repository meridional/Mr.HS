{-# LANGUAGE OverloadedStrings, DeriveGeneric #-}
module Network.MapReduce.Types where


import Data.Aeson
{-import Control.Applicative-}
{-import Control.Monad-}
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

{-
instance FromJSON WorkerCmd where
    parseJSON (Object v) = do
      l <- v .: "inputs"
      wid <- v .: "wid"
      sid <- v .: "sid"
      rc  <- v .: "rc"
      return $ WorkerCmd wid sid l rc
    parseJSON _ = mzero

instance ToJSON WorkerCmd where
    toJSON (WorkerCmd wid sid inputs rc) = object ["wid" .= show wid,
                                                   "sid" .= show sid,
                                                   "inputs" .= inputs,
                                                   "rc" .= rc]

-}

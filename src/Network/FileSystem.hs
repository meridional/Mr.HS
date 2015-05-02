{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
module Network.FileSystem where
-- XXX: place holder

class FileSystem filesys where
        type File filesys :: *
        newFile :: filesys -> String -> File filesys


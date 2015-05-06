{-|
- A sequential version of wc. used for performance comparison
-}
import Control.Monad
import qualified Data.ByteString.Lazy.Char8 as BL
import System.Environment
import Control.Applicative
import Data.List (sort, group)
import Data.ByteString.Builder
import Data.Monoid


main :: IO ()
main = do
    inputs <- getArgs
    ws <- concat <$> mapM (\f -> BL.words <$> BL.readFile f) inputs
    let wc = map (\l@(x:_) -> (x, length l)) (group (sort ws))
    forM_ wc (\(w, c) -> BL.putStrLn $ toLazyByteString $ lazyByteString w <> char8 ' ' <> intDec c)

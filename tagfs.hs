import Database.Redis.Monad.State (runWithRedis)
import Database.Redis.Monad hiding (connect)
import Database.Redis.Redis (connect)
import Control.Monad.Trans
import Control.Monad (liftM, liftM2)
import System.Fuse
import System.Environment (withArgs)
import System.Posix.Files (ownerReadMode, ownerExecuteMode, ownerWriteMode, ownerModes, getFileStatus)
import Data.Digest.OpenSSL.MD5
import qualified Data.ByteString.Char8 as BS
import Data.List (union, intersect)
import Data.Maybe (isNothing, fromJust)
import Database.Redis.ByteStringClass

------------------------------------------------------------
-- Utils
------------------------------------------------------------

getRealFilepath :: String -> String
getRealFilepath fp = "~/.tagfs/store/" ++ fp


--Relpy is RMulti filled with RBulk
responseToList :: (BS a) => Reply a -> [a]
responseToList (RMulti (Just rs)) = rbulksToList rs
    where
      rbulksToList [] = []
      rbulksToList ((RBulk Nothing):xs) = rbulksToList xs
      rbulksToList ((RBulk (Just x)):xs) = x : rbulksToList xs
responseToList _ = []

------------------------------------------------------------
-- Database stuff
------------------------------------------------------------

--differentiate keys for filenames, keys for tags
--so things don't die if people decide tagging things
--  with sha1 hashes is a good idea
filenameTag, tagTag, tempTag, filenameNameTag, hashTag :: String -> String --think up better characters, a b c  be carefull, should avoid glob specials
filenameTag f = 'a' : f --the filename hash
tagTag t = 'b' : t
tempTag t = 'c' : t
filenameNameTag f = 'd' : f
hashTag h = 'e' : h --WHAT IS THIS? I'm not sure.. filenameTag is this, surely

addFileTag :: WithRedis m => String -> String -> m ()
addFileTag f t = do
  sadd (filenameTag f) t
  sadd (tagTag t) f
  return ()

removeFileTag :: WithRedis m => String -> String -> m ()
removeFileTag f t = do
  srem (filenameTag f) t
  srem (tagTag t) f
  return ()

getTagsForFile, getFilesForTag :: (WithRedis m) => String -> m (Reply String)
getTagsForFile = smembers . filenameTag
getFilesForTag = smembers . tagTag

-- get all standard tags used for tagging files
-- we should do this in a better way, supposedly the keys command is bad
-- keep track of which tags are used, kill them when no file uses any more
-- could get messy to keep track of, 
-- could just run maintenance every so often instead
getAllTags :: WithRedis m => m ([String])
getAllTags = do
  response <- keys $ tagTag "*"
  let tags = responseToList response
  return $ map tail tags


data BooleanTree = Union BooleanTree BooleanTree 
                 | Intersect BooleanTree BooleanTree
                 | Tag String
foldBT :: WithRedis m => BooleanTree -> m ([String])
foldBT (Intersect a b) = liftM2 intersect (foldBT a) (foldBT b)
foldBT (Union a b) = liftM2 union (foldBT a) (foldBT b)
foldBT (Tag s) = liftM responseToList $ smembers s

------------------------------------------------------------
-- Fuse stuff
------------------------------------------------------------

type FH = BS.ByteString

directory, regularFile, emptyStat :: FileStat
directory = emptyStat
    { statEntryType = Directory
    , statFileSize = 4096
    , statBlocks = 1
    , statFileMode = foldr1 unionFileModes [ ownerReadMode, ownerExecuteMode ]
    }

regularFile = emptyStat
    { statEntryType = RegularFile
    , statFileMode = foldr1 unionFileModes [ ownerReadMode, ownerWriteMode ]
    }

emptyStat = FileStat
    { statEntryType = Unknown
    , statFileMode = ownerModes
    , statLinkCount = 0
    , statFileOwner = 0
    , statFileGroup = 0
    , statSpecialDeviceID = 0
    , statFileSize = 0
    , statBlocks = 0
    , statAccessTime = 0
    , statModificationTime = 0
    , statStatusChangeTime = 0
    }
-- in future use POSIX getFileStatus

openDirectory :: WithRedis m => FilePath -> m Errno
openDirectory f = return eOK

readDirectory :: WithRedis m => FilePath -> m (Either Errno [(FilePath, FileStat)])
readDirectory f = do
  let dots = [(".", directory), ("..", directory)]
  normalTags <- getAllTags
  let normalDirs = map (\t -> (t, directory)) normalTags
  
  return $ Right (dots ++ normalDirs)


getFileStat :: FilePath -> IO (Either Errno FileStat)
getFileStat f = do
  return $ Right directory --omg everything is a directory :P EEEP

--blatantly copied from FunionFS
-- what the hell is the argument even?
getFileSystemStats :: String -> IO (Either Errno FileSystemStats)
getFileSystemStats str =
  return $ Right FileSystemStats
    { fsStatBlockSize  = 512
    , fsStatBlockCount = 1
    , fsStatBlocksFree = 1
    , fsStatBlocksAvailable = 1
    , fsStatFileCount  = 5      -- IS THIS CORRECT? PROBABLY NOT
    , fsStatFilesFree  = 10     -- WHAT IS THIS? HELL IF I KNOW
    , fsStatMaxNameLength = 255 -- SEEMS SMALL? YEAH IT DOES
    }


runFuse :: Redis -> IO ()
runFuse redis = withArgs ["/tmp/tagfs2/"] $ fuseMain (fuseOps redis) defaultExceptionHandler

fuseOps :: Redis -> FuseOperations FH
fuseOps redis = defaultFuseOps { fuseOpenDirectory = (runWithRedis redis) . openDirectory
                               , fuseReadDirectory = (runWithRedis redis) . readDirectory
                               , fuseGetFileStat = getFileStat
                                                   
                               -- , fuseOpen               = undefined
                               -- , fuseFlush              = undefined
                               -- , fuseRead               = undefined
                               , fuseGetFileSystemStats = getFileSystemStats
                                                          
                               -- Dummies to make FUSE happy.
                               , fuseSetFileSize      = \_ _   -> return eOK
                               , fuseSetFileTimes     = \_ _ _ -> return eOK
                               , fuseSetFileMode      = \_ _   -> return eOK
                               , fuseSetOwnerAndGroup = \_ _ _ -> return eOK
                               }

------------------------------------------------------------
-- Testy bleh stuff
------------------------------------------------------------
main = do
  redis <- connect localhost defaultPort
  runFuse redis




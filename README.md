# splittar
Like the split command but pipes the split files into a tar so it can be piped to stdout for another program to deal with separating the files.

## why?

I know, it seems kind of nonsensical.  I'm working on cleaning up my company storage by uploading files to several cloud storage providers and some of the files are tens of gigabytes.  Because I don't want to resume failed uploads with files that large, I want the files to be split up into smaller chunks.  The GNU `split` command is what I have been using and it does what it's designed to do perfectly, but it's taking longer to split the files up on disk and then run an upload script or program to upload the directory of split files to the cloud storage provider.  This command takes a large stream, splits the stream into smaller files and then outputs those files in another tar stream that another program will read and unpack the files instead of needing an intermediate step.

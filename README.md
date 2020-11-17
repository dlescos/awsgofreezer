# awsgofreezer

A simple command line tool written in golang to upload files to AWS Glacier.

It searches for default aws credentials in ~/.aws/ %HOMEPATH%\.aws\ (see [AWS doc](https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/guide_credentials_profiles.html)).

Usage :
 -archive string
        Path to the archive to upload (MANDATORY)
 -chunksize string
        The chunk size (default 16 MiB). AWS requires it to be a power of 2
 -description string
        The archive description
 -region string
        The AWS region (default eu-central-1) (default "eu-central-1")
 -vaultname string
        The vault name (MANDATORY)
        
        
Example:

./awsgofreezer -archive ./testfile.dat -vaultname testvault


Binaries for linux and windows are compiled with -ldflags="-s -w" option and compressed with upx.

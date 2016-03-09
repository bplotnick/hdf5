/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the files COPYING and Copyright.html.  COPYING can be found at the root   *
 * of the source code distribution tree; Copyright.html can be found at the  *
 * root level of an installed copy of the electronic HDF5 document set and   *
 * is linked from the top-level documents page.  It can also be found at     *
 * http://hdfgroup.org/HDF5/doc/Copyright.html.  If you do not have          *
 * access to either file, you may request a copy from help@hdfgroup.org.     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Interface initialization */
#define H5_INTERFACE_INIT_FUNC  H5FD_s3_init_interface


#include "H5private.h"      /* Generic Functions        */
#include "H5Eprivate.h"     /* Error handling           */
#include "H5Fprivate.h"     /* File access              */
#include "H5FDprivate.h"    /* File drivers             */
#include "H5FDs3.h"       /* S3 file driver         */
#include "H5FLprivate.h"    /* Free Lists               */
#include "H5Iprivate.h"     /* IDs                      */
#include "H5MMprivate.h"    /* Memory management        */
#include "H5Pprivate.h"     /* Property lists           */
#include <libs3.h>          /* libs3           */

/* The driver identification number, initialized at runtime */
static hid_t H5FD_S3_g = 0;

static S3Protocol protocolG = S3ProtocolHTTPS;
static S3UriStyle uriStyleG = S3UriStylePath;
static const char *accessKeyIdG = 0;
static const char *secretAccessKeyG = 0;
static int statusG = 0;
static char errorDetailsG[4096] = { 0 };

/*
 * The description of a file belonging to this driver.
 * This will hold information about the object on S3
 */
typedef struct H5FD_s3_t {
    H5FD_t          pub;    /* public stuff, must be first      */
    const char           key[H5FD_MAX_FILENAME_LEN];        /* the s3 object key   */
    S3BucketContext bkt;
    haddr_t         eoa;    /* end of allocated region          */
    haddr_t         eof;    /* end of file; current file size   */
    H5FD_file_op_t  op;     /* last operation -- I dont think we need this -BP */
} H5FD_s3_t;

/*
 * These macros check for overflow of various quantities.  These macros
 * assume that HDoff_t is signed and haddr_t and size_t are unsigned.
 *
 * ADDR_OVERFLOW:   Checks whether a file address of type `haddr_t'
 *                  is too large to be represented by the second argument
 *                  of the file seek function.
 *
 * SIZE_OVERFLOW:   Checks whether a buffer size of type `hsize_t' is too
 *                  large to be represented by the `size_t' type.
 *
 * REGION_OVERFLOW: Checks whether an address and size pair describe data
 *                  which can be addressed entirely by the second
 *                  argument of the file seek function.
 */
#define MAXADDR (((haddr_t)1<<(8*sizeof(HDoff_t)-1))-1)
#define ADDR_OVERFLOW(A)    (HADDR_UNDEF==(A) || ((A) & ~(haddr_t)MAXADDR))
#define SIZE_OVERFLOW(Z)    ((Z) & ~(hsize_t)MAXADDR)
#define REGION_OVERFLOW(A,Z)    (ADDR_OVERFLOW(A) || SIZE_OVERFLOW(Z) ||    \
                                 HADDR_UNDEF==(A)+(Z) ||                    \
                                (HDoff_t)((A)+(Z))<(HDoff_t)(A))

/* Prototypes */
static H5FD_t *H5FD_s3_open(const char *name, unsigned flags, hid_t fapl_id,
            haddr_t maxaddr);
static herr_t H5FD_s3_close(H5FD_t *_file);
static haddr_t H5FD_s3_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t H5FD_s3_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr);
static haddr_t H5FD_s3_get_eof(const H5FD_t *_file);
static herr_t H5FD_s3_read(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr,
            size_t size, void *buf);
static int H5FD_s3_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t H5FD_s3_query(const H5FD_t *_f1, unsigned long *flags);


static const H5FD_class_t H5FD_s3_g = {
    "s3",                     /* name                 */
    MAXADDR,                    /* maxaddr              */
    H5F_CLOSE_WEAK,             /* fc_degree            */
    NULL,                       /* sb_size              */
    NULL,                       /* sb_encode            */
    NULL,                       /* sb_decode            */
    0,                          /* fapl_size            */
    NULL,                       /* fapl_get             */
    NULL,                       /* fapl_copy            */
    NULL,                       /* fapl_free            */
    0,                          /* dxpl_size            */
    NULL,                       /* dxpl_copy            */
    NULL,                       /* dxpl_free            */
    H5FD_s3_open,               /* open                 */
    H5FD_s3_close,              /* close                */
    H5FD_s3_cmp,                /* cmp                  */
    H5FD_s3_query,                       /* query                */
    NULL,                       /* get_type_map         */
    NULL,                       /* alloc                */
    NULL,                       /* free                 */
    H5FD_s3_get_eoa,          /* get_eoa              */
    H5FD_s3_set_eoa,          /* set_eoa              */
    H5FD_s3_get_eof,          /* get_eof              */
    NULL,                       /* get_handle           */
    H5FD_s3_read,             /* read                 */
//    H5FD_s3_write,            /* write                */
    NULL,            /* write                */
    NULL,                       /* flush                */
//    H5FD_s3_truncate,         /* truncate             */
    NULL,
    NULL,                       /* lock                 */
    NULL,                       /* unlock               */
    H5FD_FLMAP_DICHOTOMY        /* fl_map               */
};

/* Declare a free list to manage the H5FD_sec2_t struct */
H5FL_DEFINE_STATIC(H5FD_s3_t); //?


/*-------------------------------------------------------------------------
 * Function:    H5FD_s3_init_interface
 *
 * Purpose:     Initializes any interface-specific data or routines.
 *
 * Return:      Success:    The driver ID for the s3 driver.
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_s3_init_interface(void)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    FUNC_LEAVE_NOAPI(H5FD_s3_init())
} /* H5FD_s3_init_interface() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_s3_init
 *
 * Purpose:     Initialize this driver by registering the driver with the
 *              library.
 *
 * Return:      Success:    The driver ID for the s3 driver.
 *              Failure:    Negative
 *
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5FD_s3_init(void)
{
    hid_t ret_value;            /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if(H5I_VFL != H5Iget_type(H5FD_S3_g))
       //H5FD_S3_g = H5FDregister(&H5FD_s3_g, sizeof(H5FD_class_t), FALSE);
       H5FD_S3_g = H5FDregister(&H5FD_s3_g);

    S3Status status;
    const char *hostname = getenv("S3_HOSTNAME");

    if ((status = S3_initialize("s3", S3_INIT_ALL, hostname))
        != S3StatusOK) {
       fprintf(stderr, "Failed to initialize libs3: %s\n",
               S3_get_status_name(status));
       exit(-1);
    }


    // For now we'll just set it from environment variables
    accessKeyIdG = getenv("S3_ACCESS_KEY_ID");
    if (!accessKeyIdG) {
       fprintf(stderr, "Missing environment variable: S3_ACCESS_KEY_ID\n");
       return -1;
    }
    secretAccessKeyG = getenv("S3_SECRET_ACCESS_KEY");
    if (!secretAccessKeyG) {
       fprintf(stderr,
               "Missing environment variable: S3_SECRET_ACCESS_KEY\n");
       return -1;
    }

    
    /* Set return value */
    ret_value = H5FD_S3_g;
    
done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_s3_init() */


/*---------------------------------------------------------------------------
 * Function:    H5FD_s3_term
 *
 * Purpose:     Shut down the VFD
 *
 * Returns:     <none>
 *
 *
 *---------------------------------------------------------------------------
 */
void
H5FD_s3_term(void)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR
       
    S3_deinitialize();
       
    /* Reset VFL ID */
    H5FD_S3_g = 0;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5FD_s3_term() */


/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_s3
 *
 * Purpose:     Modify the file access property list to use the H5FD_s3
 *              driver defined in this source file.
 *
 * Return:      SUCCEED/FAIL
 *
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_s3(hid_t fapl_id)
{
    herr_t ret_value;

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", fapl_id);

    ret_value = H5Pset_driver(fapl_id, H5FD_S3, NULL);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fapl_s3() */


static void printError(void)
{
   if (statusG < S3StatusErrorAccessDenied) {
      fprintf(stderr, "\nERROR: %s\n", S3_get_status_name(statusG));
   }
   else {
      fprintf(stderr, "\nERROR: %s\n", S3_get_status_name(statusG));
      fprintf(stderr, "%s\n", errorDetailsG);
   }
}


// Generic responsePropertiesCallback
static S3Status responsePropertiesCallback
(const S3ResponseProperties *properties, void *callbackData)
{

   return S3StatusOK;
}

// This callback does the same thing for every request type: saves the status
// and error stuff in global variables
// FIXME: Why don't we just pass the S3Status back in the callbackData?
static void responseCompleteCallback(S3Status status,
                                     const S3ErrorDetails *error,
                                     void *callbackData)
{
   (void) callbackData;

   statusG = status;
   // Compose the error details message now, although we might not use it.
   // Can't just save a pointer to [error] since it's not guaranteed to last
   // beyond this callback
   unsigned int len = 0;
   if (error && error->message) {
      len += snprintf(&(errorDetailsG[len]), sizeof(errorDetailsG) - len,
                      "  Message: %s\n", error->message);
   }
   if (error && error->resource) {
      len += snprintf(&(errorDetailsG[len]), sizeof(errorDetailsG) - len,
                      "  Resource: %s\n", error->resource);
   }
   if (error && error->furtherDetails) {
      len += snprintf(&(errorDetailsG[len]), sizeof(errorDetailsG) - len,
                      "  Further Details: %s\n", error->furtherDetails);
   }
   if (error && error->extraDetailsCount) {
      len += snprintf(&(errorDetailsG[len]), sizeof(errorDetailsG) - len,
                      "%s", "  Extra Details:\n");
      int i;
      for (i = 0; i < error->extraDetailsCount; i++) {
         len += snprintf(&(errorDetailsG[len]),
                         sizeof(errorDetailsG) - len, "    %s: %s\n",
                         error->extraDetails[i].name,
                         error->extraDetails[i].value);
      }
   }
}

static S3Status getObjectDataCallback(int bufferSize, const char *buffer,
                                      void *callbackData)
{
   char **buf = (char **) callbackData;

   memcpy(*buf, buffer, bufferSize);
   *buf += bufferSize;
   callbackData = (void*) buf;
   return S3StatusOK;
}


// Just like regular responsePropertiesCallback, except it will put the contentlen in the callbackdata
static S3Status responsePropertiesCallbackContentlen
(const S3ResponseProperties *properties, void *callbackData)
{
   // assert(calllbackData != NULL)
   unsigned long long* data = (unsigned long long*) callbackData;
   *data = properties->contentLength;
   
   return S3StatusOK;
}
   


/*-------------------------------------------------------------------------
 * Function:    H5FD_s3_open
 *
 * Purpose:     This will primarily store key and do minimal checking
 *
 * Return:      Success:    A pointer to a new file data structure. The
 *                          public fields will be initialized by the
 *                          caller, which is always H5FD_open().
 *              Failure:    NULL
 *
 *
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD_s3_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr)
{
    H5FD_s3_t     *file       = NULL;     /* s3 VFD info            */
    H5FD_t          *ret_value;             /* Return value             */
    unsigned long long contentLength;
    
    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity check on file offsets */
    //HDcompile_assert(sizeof(HDoff_t) >= sizeof(size_t));

    /* Check arguments */
    if(!name || !*name)
       HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid file name")
    /*
    if(0 == maxaddr || HADDR_UNDEF == maxaddr)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, NULL, "bogus maxaddr")
    if(ADDR_OVERFLOW(maxaddr))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, NULL, "bogus maxaddr")
    */

    

    /* Create the new file struct */
    if(NULL == (file = H5FL_CALLOC(H5FD_s3_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "unable to allocate file struct")
    if(H5P_FILE_ACCESS_DEFAULT==fapl_id || H5FD_S3!=H5Pget_driver(fapl_id)) {
       //FIXME: Figure out bucket name from filename and call H5Pset_fapl_s3()
       HGOTO_ERROR(H5E_VFL, H5E_OPENERROR, NULL, "Can't open. You must first call set_fapl_s3")
    }

    //TODO: make sure that the s3:// part isn't on the filename
    char cp[H5FD_MAX_FILENAME_LEN];
    char* saveptr;
    HDstrcpy(cp,name);
    // Split off bucket name
    char* bucketName = HDcalloc(S3_MAX_BUCKET_NAME_SIZE,sizeof(char));
    HDstrcpy(bucketName,HDstrtok_r(cp,"/",&saveptr)); // saveptr points to the rest of the string
    S3BucketContext bucketContext =
       {
          0,
          bucketName,
          protocolG,
          uriStyleG,
          accessKeyIdG,
          secretAccessKeyG
       };
    
    //TODO: Do minimal access checks on key (I guess we are doing a head() so it's built in.
    file->bkt = bucketContext;
    HDstrcpy(file->key,saveptr);
    file->op = OP_UNKNOWN;

    S3ResponseHandler responseHandler =
    {
       &responsePropertiesCallbackContentlen,
       &responseCompleteCallback
    };
    do {
       S3_head_object(&file->bkt, file->key, 0, &responseHandler, &contentLength);
    } while (S3_status_is_retryable(statusG));

    if ((statusG != S3StatusOK) &&
        (statusG != S3StatusErrorPreconditionFailed)) {
       printError();
    }

    file->eof = contentLength;
    
    /* Set return value */
    ret_value = (H5FD_t*)file;

done:
    if(NULL == ret_value) {
        if(file)
            file = H5FL_FREE(H5FD_s3_t, file);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_s3_open() */



/*-------------------------------------------------------------------------
 * Function:    H5FD_s3_close
 *
 * Purpose:     Closes an HDF5 file.
 *
 * Return:      Success:    SUCCEED
 *              Failure:    FAIL, file not closed.
 *
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_s3_close(H5FD_t *_file)
{
    H5FD_s3_t *file = (H5FD_s3_t *)_file;
    herr_t      ret_value = SUCCEED;                /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity check */
    HDassert(file);

    // FIXME: Need helper functions to allocate and deallocate bucketcontext
    HDfree((void*)file->bkt.bucketName);
    /* Release the file info */
    file = H5FL_FREE(H5FD_s3_t, file);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_s3_close() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_s3_cmp
 *
 * Purpose:     Compares two files belonging to this driver using an
 *              arbitrary (but consistent) ordering.
 *
 * Return:      Success:    A value like strcmp()
 *              Failure:    never fails (arguments were checked by the
 *                          caller).
 *
 *
 *-------------------------------------------------------------------------
 */
static int
H5FD_s3_cmp(const H5FD_t *_f1, const H5FD_t *_f2)
{
    const H5FD_s3_t   *f1 = (const H5FD_s3_t *)_f1;
    const H5FD_s3_t   *f2 = (const H5FD_s3_t *)_f2;
    int ret_value = 0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    ret_value = strcmp(f1->bkt.bucketName,f2->bkt.bucketName);
    if (ret_value != 0)
       HGOTO_DONE(ret_value);

    ret_value = strcmp(f1->key,f2->key);
    
done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_s3_cmp() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_s3_query
 *
 * Purpose:     Set the flags that this VFL driver is capable of supporting.
 *              (listed in H5FDpublic.h)
 *
 * Return:      SUCCEED (Can't fail)
 *
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_s3_query(const H5FD_t *_file, unsigned long *flags /* out */)
{
    const H5FD_s3_t	*file = (const H5FD_s3_t *)_file;    /* s3 VFD info */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Set the VFL feature flags that this driver supports */
    if(flags) {
        *flags = 0;
        *flags |= H5FD_FEAT_AGGREGATE_METADATA;     /* OK to aggregate metadata allocations                             */
        *flags |= H5FD_FEAT_ACCUMULATE_METADATA;    /* OK to accumulate metadata for faster writes                      */
        *flags |= H5FD_FEAT_DATA_SIEVE;             /* OK to perform data sieving for faster raw data reads & writes    */
        *flags |= H5FD_FEAT_AGGREGATE_SMALLDATA;    /* OK to aggregate "small" raw data allocations                     */
//        *flags |= H5FD_FEAT_POSIX_COMPAT_HANDLE;    /* VFD handle is POSIX I/O call compatible                          */

        /* Check for flags that are set by h5repart */
//        if(file && file->fam_to_s3)
//            *flags |= H5FD_FEAT_IGNORE_DRVRINFO; /* Ignore the driver info when file is opened (which eliminates it) */
    } /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD_s3_query() */



/*-------------------------------------------------------------------------
 * Function:    H5FD_s3_get_eoa
 *
 * Purpose:     Gets the end-of-address marker for the file. The EOA marker
 *              is the first address past the last byte allocated in the
 *              format address space.
 *
 * Return:      The end-of-address marker.
 *

 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD_s3_get_eoa(const H5FD_t *_file, H5FD_mem_t UNUSED type)
{
    const H5FD_s3_t	*file = (const H5FD_s3_t *)_file;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    FUNC_LEAVE_NOAPI(file->eoa)
} /* end H5FD_s3_get_eoa() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_s3_set_eoa
 *
 * Purpose:     Set the end-of-address marker for the file. This function is
 *              called shortly after an existing HDF5 file is opened in order
 *              to tell the driver where the end of the HDF5 data is located.
 *
 * Return:      SUCCEED (Can't fail)
 *
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_s3_set_eoa(H5FD_t *_file, H5FD_mem_t UNUSED type, haddr_t addr)
{
    H5FD_s3_t	*file = (H5FD_s3_t *)_file;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    file->eoa = addr;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD_s3_set_eoa() */


/*-------------------------------------------------------------------------
 * Function:    H5FD_s3_get_eof
 *
 * Purpose:     Returns the end-of-file marker, which is the greater of
 *              either the filesystem end-of-file or the HDF5 end-of-address
 *              markers.
 *
 * Return:      End of file address, the first address past the end of the 
 *              "file", either the filesystem file or the HDF5 file.
 *
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD_s3_get_eof(const H5FD_t *_file)
{
    const H5FD_s3_t   *file = (const H5FD_s3_t *)_file;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    FUNC_LEAVE_NOAPI(MAX(file->eof, file->eoa))
} /* end H5FD_s3_get_eof() */



/*-------------------------------------------------------------------------
 * Function:    H5FD_s3_read
 *
 * Purpose:     Reads SIZE bytes of data from FILE beginning at address ADDR
 *              into buffer BUF according to data transfer properties in
 *              DXPL_ID.
 *
 * Return:      Success:    SUCCEED. Result is stored in caller-supplied
 *                          buffer BUF.
 *              Failure:    FAIL, Contents of buffer BUF are undefined.
 *
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_s3_read(H5FD_t *_file, H5FD_mem_t UNUSED type, hid_t UNUSED dxpl_id,
    haddr_t addr, size_t size, void *buf /*out*/)
{
    H5FD_s3_t     *file       = (H5FD_s3_t *)_file;
    herr_t          ret_value   = SUCCEED;                  /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(file && file->pub.cls);
    HDassert(buf);

    /* Check for overflow conditions */
    if(!H5F_addr_defined(addr))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "addr undefined, addr = %llu", (unsigned long long)addr)
    if(REGION_OVERFLOW(addr, size))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "addr overflow, addr = %llu", (unsigned long long)addr)

           
    S3GetObjectHandler getObjectHandler =
    {
        { &responsePropertiesCallback, &responseCompleteCallback },
              &getObjectDataCallback
    };
    

    // Here we save the beginning of the buffer, which is used as a counter in the data callback function
    void *startofbuf = buf;
    do {
       S3_get_object(&file->bkt, file->key, NULL, addr,
                     (uint64_t)size, 0, &getObjectHandler, &buf);
    } while (S3_status_is_retryable(statusG));

    if (statusG != S3StatusOK) {
       printError();
       ret_value = -1;
    }
    buf = startofbuf;
    
    /* Update current position */
    file->op = OP_READ;

done:
    if(ret_value < 0) {
        /* Reset last file I/O information */
        file->op = OP_UNKNOWN;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_s3_read() */

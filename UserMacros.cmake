########################################################
#  Include file for user options
########################################################

#-----------------------------------------------------------------------------
#------------------- E X A M P L E   B E G I N--------------------------------
#-----------------------------------------------------------------------------
# Option to Build with User Defined Values
#-----------------------------------------------------------------------------
MACRO (MACRO_USER_DEFINED_LIBS)
  set (USER_DEFINED_VALUE "FALSE")
ENDMACRO (MACRO_USER_DEFINED_LIBS)

#-------------------------------------------------------------------------------
option (BUILD_USER_DEFINED_LIBS "Build With User Defined Values" OFF)
if (BUILD_USER_DEFINED_LIBS)
  MACRO_USER_DEFINED_LIBS ()
endif (BUILD_USER_DEFINED_LIBS)

option (HDF5_ENABLE_S3_SUPPORT "Enable S3 VFD" ON)
if (HDF5_ENABLE_S3_SUPPORT)
   find_package(LIBS3 NAMES libs3 s3)
   if (NOT LIBS3_FOUND)
     find_library(LIBS3_LIBRARY NAMES s3 libs3 HINTS "${CMAKE_PREFIX_PATH}/lib")
     find_path(LIBS3_INCLUDE_DIR libs3.h HINTS "${CMAKE_PREFIX_PATH}/include")
   endif (NOT LIBS3_FOUND)
   set (LINK_LIBS ${LINK_LIBS} ${LIBS3_LIBRARY})
   INCLUDE_DIRECTORIES (${LIBS3_INCLUDE_DIR})
endif (HDF5_ENABLE_S3_SUPPORT)
#-----------------------------------------------------------------------------
#------------------- E X A M P L E   E N D -----------------------------------
#-----------------------------------------------------------------------------
 
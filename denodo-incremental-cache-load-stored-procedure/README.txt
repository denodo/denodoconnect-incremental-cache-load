==============================================================================
DENODO INCREMENTAL CACHE LOAD
==============================================================================

The Denodo Incremental Cache Load is a tool that allows the execution of an
incremental cache load on demand. It saves the current scenario of having to 
perform two queries with one insert into cache per new/modified tuple to be cached. 
The SP can perform this same operation in blocks, by asking VDP to update the 
cache with SELECT queries using large blocks of "IN" conditions (as large as 
allowed by the cached source) defined in the parameters of the execution.

In order to run the Denodo Incremental Cache Load Stored Procedure you need to
import to the Virtual DataPort Server the JAR file inluded in /dist folder:

denodo-incremental-cache-load-{vdpversion}-{version}-jar-with-dependencies.jar 

After adding the stored procedure using the JAR file previously added and 
setting 'com.denodo.connect.cache.IncrementalCacheLoadStoreProcedure' as class 
name value, you can invoke it to run the process. 

Input parameters:
  DATABASE_NAME: non-nullable text 
  VIEW_NAME: non-nullable text  
  LAST_UPDATE_CONDITION: non-nullable text 
  NUM_ELEMENTS_IN_CLAUSE: nullable text 
  
Output parameter:
  NUM_UPDATED_ROWS

For more info, see the User Manual at the /doc folder.

This software is part of the DenodoConnect component collection.

Copyright (c) 2018, denodo technologies (http://www.denodo.com)
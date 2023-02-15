# ziwa

Ziwa is a knowledge storage and transformation engine.

## Concepts

  * Blob - An output from a Job, e.g: the contents of a binary file, or a parquet transformation.
  * Filter - A query that selects files.
  * Job - An instance of a processor working on an input Blob, which has a state machine tracking its completeted, incomplete, in progress states.
  * Lake - The collection of all files, can be added to.
  * Processor - Is a recipe for how to take an input and produce an output.
  * Route - Takes a filter and creates jobs any time a Blob is added.

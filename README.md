# ziwa

Ziwa is a knowledge storage and transformation engine.

## Concepts

Job - Is a recipe for how to take an input and produce an output.
File - A single unit of knowledge, a container for data.
Filter - A query that selects files.
Route - Takes a filter and creates jobs any time a File is added.
Lake - The collection of all files, can be added to.

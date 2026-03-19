# Split Oversized Files

Many files inour project have become too long for AI to efficiently work on them.

This plan should be executed repeatedly by sub-agents until no over-sized files remain.

## 0. (DO ONCE) Create A Test

Create a test that locates any python, javascript or html file inside of src/gpdb and gpdb_admin/src/gpdb that is greater than the maximum length (750 lines).

## 1. Locate an Oversized Code File

Run the test to locate the first file that exceeds the allowable length. This is your focus for the remaining steps in this plan.

## 2. Plan The Split

Determine an intelligent, logical, way to split the file up. It should remain organized, the resulting file names should clearly delineate what is in them, and it should be clear how to continue growing in the future, such that we are not just splitting the big file, but also creating an ongoing organized set of files for growth in the future.

## 3. Perform the Split

Separate out the file into the multiple componenets.

## 4. Ensure Tests Pass

We are not changing any functionality, just the organization of internal files and importables - so after splitting the file, ensure appropriate changes to other imports etc have been made and that all tests once again pass.

# When there are no code files in the project that are longer than 750 lines

Stop and report to the orchestrator agent that you cannot find any more files, and therefore, that all files are now < 750 lines.
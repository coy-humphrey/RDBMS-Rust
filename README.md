# Relational Database Management System - Rust

This project implements a toy relational database management system in Rust. It follows the "Rewrite it in Rust" mentality, and is based on a project I wrote in C++ when I was the TA for CMPS181 - Database Management Systems II during spring quarter 2016 at UC Santa Cruz. Original project here: https://github.com/coy-humphrey/RDBMS-Cpp/

The system will consist of the following components:

* Paged File Manager (PFM) (Complete)
* * Allows reading and writing entire pages at once based on a page index.
* Record Based File Manager (RBFM) (In-progress)
* * Given data and a record format, writes the data to disk as a record.
* * Records are indexed by a page number and a slot number within a page.
* Relation Manager (RM) (Not yet started)
* * Manages tables, including column information.
* * Used to define the record format used by the RBFM.
* * Ensures changes to RBFM layer are reflected in associated Indexes
* Index Manager (IX) (Not yet started)
* * Given a table and a column from that table, creates an index to allow quick lookups and in order traversals.
* * Index is implemented as an on-disk B+ tree.
* Query Engine (QE) (Not yet started)
* * Implements query operations such as Filter, Project, and Join (equality only for join condition)
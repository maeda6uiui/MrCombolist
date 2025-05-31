# MrCombolist

> Once you walk off the paved leafy avenue and step into a back alley of the Internet, you'll find yourself in shambles.
> But I find joy in collecting some craps out there, polishing them at home, and then decorating my petty shelf with them.

## Overview

This repo is for the scripts to work with combolists and (maybe) for analysis results obtained from them.
It's subject to frequent change, and I don't have any plans to get it production-ready.
This is just a hobby project for my free time. :-)

My current objective is to compile large lists of most common passwords.

## Requirements

[uv](https://github.com/astral-sh/uv) is used to manage this project.
See their readme to find out how to install it.

After you install uv, just run `uv sync` to set up a virtual environment for this project.

### Operation flow

![Operation flow](./Image/operation_flow.png)

### Terminology

#### PoH

PoH stands for "Password or Hash".
Combolists contain hashed passwords as well as plaintext ones.

#### Persona

Persona (pl. personae) is a set of data related to an email address.
Currently, it is a set of email address and poh.
I've started to use this term out of a whim, so it may be changed in case any better wordage is found.

## Notes

### 2024-04-07

Obtained 2,616,227,379 personae from the *Collection 1* dataset.
Here are 10 most frequent pohs of the dataset:

```
> SELECT word,freq FROM freqs ORDER BY freq DESC LIMIT 10;
(123456, 18442419)
(123456789, 9448028)
('password', 5054888)
('qwerty', 4233883)
(12345, 3265726)
(12345678, 3187054)
('qwerty123', 1984784)
(1234567890, 1831612)
(111111, 1720032)
('1q2w3e', 1694497)
```

10 most frequent pohs of the *Cit0day* dataset, but this time with a condition of `LENGTH(word)<=24` in hopes of excluding hash values:

```
> SELECT word,freq FROM freqs WHERE LENGTH(word)<=24 ORDER BY freq DESC LIMIT 10;
(123456, 628209)
('thehatch', 247991)
(123456789, 148713)
('password', 118624)
('123abc', 109693)
(12345, 109306)
('x4ivygA51F', 97462)
('D1lakiss', 89666)
(1234, 81785)
(0, 81277)
```

### 2024-04-06

Updated some scripts and added an image explaining the current operation flow.
Also, just resumed working with the *Collection 1* dataset.

### 2024-01-14

Procedure update

1. Unarchive (unarchive.py)
2. Flatten (flatten.py)
3. Split into chunks (split.py)
4. Regroup (regroup.py)
5. Rearchive (rearchive.py)
6. Detect schema (detect_schema.py)
7. Parse (parse.py)
8. Cleanup (cleanup.py)
9. Convert to Parquet (to_parquet.py)
10. Count local frequencies (count_local_freqs.py)
11. Convert to SQL (SQLite DB) (to_sql.py)
12. Count dataset frequencies (count_dataset_freqs.py)

### 2024-01-08

Created SQLite DBs from the *Cit0day* dataset.
Below is a sample query to get top 10 most frequent PoH's (Password or Hash).

```
> SELECT word,freq FROM dataset_freqs ORDER BY freq DESC LIMIT 10;
('2C50B4105AAC4C8BF2BB53A2481587DFA3B272E6', 642075)
(123456, 628209)
('da39a3ee5e6b4b0d3255bfef95601890afd80709', 522394)
('thehatch', 247991)
('d41d8cd98f00b204e9800998ecf8427e', 231404)
('e10adc3949ba59abbe56e057f20f883e', 203910)
('e10adc3949ba59abbe56e057f20f883ed93a5def7511da3d0f2d171d9c344e91', 153303)
(123456789, 148713)
('c6d326e212f4f006100ce0f721559aaf', 146382)
('password', 118624)
```

### 2024-01-01

Currently testing to process a combolist with below procedure.

1. Unarchive
2. Flatten
3. Regroup
4. Rearchive
5. Detect Schema
6. Parse
7. Convert to Parquet

### 2023-12-30

Currently overhauling the procedure and the scripts...


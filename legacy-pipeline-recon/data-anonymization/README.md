# Data Anonymization

## Description

This part of the pipeline will do data anonymization for data public release and monitoring dashboards.

## Method

-   Use sha256 hashing for anonymizing PII IDs from our survey forms using R `digest` library
-   Create hash map table to de-crypt anonymized columns (only shared internally)
-   Remove original ID Cols and replace it with new hash column for public data release
-   Save anonymized data under `public-form`

### Why use SHA-256 (or known as SHA-2):

There are 3 common cryptographic techniques such as MD5, SHA-1, SHA-2. SHA-2 is known to be the most secure, despite computationally 20-30% longer to calculate compared the other two. It is irreversible, so the only way to get PII Ids are through doing `hash join` with the non-anonymized / original table.

### How to do Hash Joins to decrypt
``` r
non_anonymized_form %>%
    dplyr::select(id) %>%
    dplyr::mutate(id = digest(id, algo = "sha256")) %>% 
    dplyr::inner_join(anonymized_forms, by = c("id"))
```

## Anonymized Forms

:white_check_mark: Kenya Household Forms

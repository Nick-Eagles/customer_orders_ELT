gold.dim_customer = silver.crm_cust_info |>
    select(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    ) |>
    rename(
        crm_key_1 = cst_id,
        crm_key_2 = cst_key,
        first_name = cst_firstname,
        last_name = cst_lastname,
        marital_status = cst_marital_status,
        gender = cst_gndr,
        create_date = cst_create_date
    ) |>
    left_join(
        silver.erp_cust_az12 |>
            select(cid, cid_clean, bdate, gen) |>
            rename(erp_key_1 = cid, birth_date = bdate),
        by = c("crm_key_1" = "cid_clean")
    ) |>
    mutate(
        gender = case_when(
            is.na(gender) ~ gen,
            gender != gen ~ NA,
            TRUE ~ gender
        )
    ) |>
    left_join(
        silver.erp_loc_a101 |>
            select(cid, cid_clean, cntry) |>
            rename(erp_key_2 = cid, country = cntry) |>
        by = c("crm_key_1" = "cid_clean")
    ) |>
    mutate(surrogate_key = row_number()) |>
    select(
        surrogate_key,
        crm_key_1,
        crm_key_2,
        erp_key_1,
        erp_key_2,
        first_name,
        last_name,
        marital_status,
        gender,
        create_date,
        birth_date,
        country
    )

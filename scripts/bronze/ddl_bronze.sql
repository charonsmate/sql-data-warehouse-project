


/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from CSV files to bronze tables.

Parameters:
    p_base_path TEXT - Ruta base donde se encuentran los archivos CSV
                       Default: 'C:/Users/sergi/Documents/DataAnalysisEngineering/sql-data-warehouse-project/datasets'

Usage Example:
    CALL bronze.load_bronze();
    -- O con ruta personalizada:
    CALL bronze.load_bronze('D:/MiProyecto/datasets');
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze(
    p_base_path TEXT DEFAULT 'C:/Users/sergi/Documents/DataAnalysisEngineering/sql-data-warehouse-project/datasets'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_batch_start_time TIMESTAMP;
    v_batch_end_time TIMESTAMP;
    v_duration INTERVAL;
    v_total_duration INTERVAL;
    v_error_msg TEXT;
BEGIN
    v_batch_start_time := CLOCK_TIMESTAMP();
    
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    BEGIN
        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE 'Loading CRM Tables';
        RAISE NOTICE '------------------------------------------------';

        -- Load crm_cust_info
        v_start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
        EXECUTE format(
            'COPY bronze.crm_cust_info FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', QUOTE ''"'', ENCODING ''UTF8'')',
            p_base_path || '/source_crm/cust_info.csv'
        );
        
        v_end_time := CLOCK_TIMESTAMP();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
        RAISE NOTICE '>> Rows Loaded: %', (SELECT COUNT(*) FROM bronze.crm_cust_info);
        RAISE NOTICE '>> -------------';

        -- Load crm_prd_info
        v_start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
        EXECUTE format(
            'COPY bronze.crm_prd_info FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', QUOTE ''"'', ENCODING ''UTF8'')',
            p_base_path || '/source_crm/prd_info.csv'
        );
        
        v_end_time := CLOCK_TIMESTAMP();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
        RAISE NOTICE '>> Rows Loaded: %', (SELECT COUNT(*) FROM bronze.crm_prd_info);
        RAISE NOTICE '>> -------------';

        -- Load crm_sales_details
        v_start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
        EXECUTE format(
            'COPY bronze.crm_sales_details FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', QUOTE ''"'', ENCODING ''UTF8'')',
            p_base_path || '/source_crm/sales_details.csv'
        );
        
        v_end_time := CLOCK_TIMESTAMP();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
        RAISE NOTICE '>> Rows Loaded: %', (SELECT COUNT(*) FROM bronze.crm_sales_details);
        RAISE NOTICE '>> -------------';

        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE 'Loading ERP Tables';
        RAISE NOTICE '------------------------------------------------';

        -- Load erp_loc_a101
        v_start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
        EXECUTE format(
            'COPY bronze.erp_loc_a101 FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', QUOTE ''"'', ENCODING ''UTF8'')',
            p_base_path || '/source_erp/loc_a101.csv'
        );
        
        v_end_time := CLOCK_TIMESTAMP();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
        RAISE NOTICE '>> Rows Loaded: %', (SELECT COUNT(*) FROM bronze.erp_loc_a101);
        RAISE NOTICE '>> -------------';

        -- Load erp_cust_az12
        v_start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
        EXECUTE format(
            'COPY bronze.erp_cust_az12 FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', QUOTE ''"'', ENCODING ''UTF8'')',
            p_base_path || '/source_erp/cust_az12.csv'
        );
        
        v_end_time := CLOCK_TIMESTAMP();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
        RAISE NOTICE '>> Rows Loaded: %', (SELECT COUNT(*) FROM bronze.erp_cust_az12);
        RAISE NOTICE '>> -------------';

        -- Load erp_px_cat_g1v2
        v_start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        EXECUTE format(
            'COPY bronze.erp_px_cat_g1v2 FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','', QUOTE ''"'', ENCODING ''UTF8'')',
            p_base_path || '/source_erp/px_cat_g1v2.csv'
        );
        
        v_end_time := CLOCK_TIMESTAMP();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
        RAISE NOTICE '>> Rows Loaded: %', (SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2);
        RAISE NOTICE '>> -------------';

        v_batch_end_time := CLOCK_TIMESTAMP();
        v_total_duration := v_batch_end_time - v_batch_start_time;
        
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'Loading Bronze Layer is Completed';
        RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM v_total_duration)::INTEGER;
        RAISE NOTICE '==========================================';

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT;
            
            RAISE NOTICE '==========================================';
            RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
            RAISE NOTICE 'Error Message: %', v_error_msg;
            RAISE NOTICE 'Error Code: %', SQLSTATE;
            RAISE NOTICE '==========================================';
            
            -- Re-lanzar el error para que se propague
            RAISE;
    END;
END;
$$;

-- Agregar comentario al procedimiento
COMMENT ON PROCEDURE bronze.load_bronze IS 'Carga datos desde archivos CSV a las tablas de la capa Bronze (CRM y ERP)';

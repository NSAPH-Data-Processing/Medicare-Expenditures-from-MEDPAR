import pandas as pd
import argparse
import duckdb
import pyarrow.parquet as pq

def prep_df(
    year,
    adm_expenditures_prefix, 
    bene_prefix, 
    zip_data_prefix,
    conn):

    query = f"""
    WITH adm AS(
        SELECT 
            bene_id,
            year,
            SUM(length_of_stay_days) as length_of_stay_days, 
            SUM(total_medicare_payment) as total_medicare_payment
        FROM '{adm_expenditures_prefix}_{year}.parquet'
        GROUP BY bene_id, year
    ), bene AS(
        SELECT
            bene_id,
            zip,
            year,
            age_dob as age,
            sex, 
            race, 
            dual,
            hmo_mo
        FROM '{bene_prefix}_{year}.parquet'
    ), zip AS (
        SELECT * 
        from '{zip_data_prefix}_{year}.parquet'
    )
    SELECT 
        b.bene_id, 
        b.zip, 
        b.year, 
        b.age, 
        b.sex, 
        b.race, 
        b.dual,
        b.hmo_mo,
        b.length_of_stay_days, 
        b.total_medicare_payment, 
        zip.pm25_ensemble, 
        zip.mean_bmi, 
        zip.smoke_rate, 
        zip.hispanic, 
        zip.pct_blk,
        zip.medhouseholdincome, 
        zip.medianhousevalue, 
        zip.poverty, 
        zip.education,
        zip.popdensity, 
        zip.pct_owner_occ, 
        zip.summer_tmmx, 
        zip.winter_tmmx,
        zip.summer_rmax, 
        zip.winter_rmax, 
        zip.region
    FROM (
        SELECT
            bene.bene_id,
            bene.zip,
            bene.year,
            bene.age,
            bene.sex,
            bene.race, 
            bene.dual,
            bene.hmo_mo,
            COALESCE(adm.length_of_stay_days, 0) as length_of_stay_days,
            COALESCE(adm.total_medicare_payment, 0) as total_medicare_payment
        FROM bene
        LEFT JOIN adm
        ON 
            bene.bene_id = adm.bene_id AND 
            bene.year = adm.year
    ) as b
    LEFT JOIN zip
    ON 
        b.zip = zip.zip AND 
        b.year = zip.year
    """
    df = conn.execute(query).fetchdf()

    return df

def main(args):
    
    conn = duckdb.connect()

    # # read parquet column names
    # cols = pq.read_table(f"{args.adm_expenditures_prefix}_{args.year}.parquet").column_names
    # print(cols)
    # cols = pq.read_table(f"{args.dw_bene_prefix}_{args.year}.parquet").column_names
    # print(cols)
    # cols = pq.read_table(f"{args.zip_data_prefix}_{args.year}.parquet").column_names
    # print(cols)

    print("## Preparing data ----")
    df = prep_df(
        args.year,
        args.adm_expenditures_prefix,
        args.dw_bene_prefix,
        args.zip_data_prefix,
        conn)
    
    print("## Set index ----")
    df = df.set_index(['bene_id'])

    print("## Write data ----")
    output_file = f"{args.output_prefix}_{args.year}.{args.output_format}"
    if args.output_format == "parquet":
        df.to_parquet(output_file)
    elif args.output_format == "feather":
        df.to_feather(output_file)
    elif args.output_format == "csv":
        df.to_csv(output_file)

    print(f"## Output file written to {output_file}")
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", 
                        default = 2000, 
                        type=int
                       )
    parser.add_argument("--adm_expenditures_prefix", 
                        default = "../data/intermediate/scratch/adm_expenditures"
                       )
    parser.add_argument("--zip_data_prefix", 
                        default = "../data/intermediate/scratch/zip_data"
                       )
    parser.add_argument("--dw_bene_prefix", 
                        default = "../data/input/dw_bene_xu_sabath_00_16/bene"
                       )
    parser.add_argument("--output_format", 
                        default = "parquet", 
                        choices=["parquet", "feather", "csv"]
                       )           
    parser.add_argument("--output_prefix", 
                    default = "../data/output/medicare_expenditures/medicare_expenditures"
                   )
    args = parser.parse_args()
    
    main(args)

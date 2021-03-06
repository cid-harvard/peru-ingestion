import pandas as pd

store = pd.HDFStore("data.h5")


classifications = {}
for key in store:
    if key.startswith('/classifications/'):

        name = store.get_storer(key).attrs.atlas_metadata['sql_table_name']
        table = store[key]

        # Customize table to be ready for merging
        table = table.set_index("index")
        if name == "location":
            table = table[["code", "name"]]
        else:
            table = table[["code", "name", "name_es"]]
        table.columns = [name + "_" + col for col in table.columns]

        classifications[name + "_id"] = table


def merge_classifications(df):
    for col in df.columns:
        if col in classifications:
            df = df.merge(classifications[col], left_on=col, right_index=True)
            df = df.drop(col, axis=1)
    return df

py = store['product_year'][['product_id', 'year', 'pci']]

location_year_columns = ['location_id', 'year', 'eci', 'coi']
cy = store['country_year'][location_year_columns]
dy = store['department_year'][location_year_columns]
my = store['msa_year'][location_year_columns]

pd.set_option("io.excel.xlsx.writer", "xlsxwriter")

cpy = store['country_product_year']\
    .merge(cy, on=['location_id', 'year'])\
    .merge(py, on=['product_id', 'year'])
merge_classifications(cpy)\
    .to_excel("downloads/products_country.xlsx", index=False)


dpy = store['department_product_year']\
    .merge(dy, on=['location_id', 'year'])\
    .merge(py, on=['product_id', 'year'])
merge_classifications(dpy)\
    .to_excel("downloads/products_department.xlsx", index=False)


ppy = store['msa_product_year']\
    .merge(my, on=['location_id', 'year'])\
    .merge(py, on=['product_id', 'year'])
merge_classifications(ppy)\
    .to_excel("downloads/products_province.xlsx", index=False)


ccpy = store['country_country_product_year']\
    .merge(cy, on=['location_id', 'year'])\
    .merge(py, on=['product_id', 'year'])\
    .merge(cpy[["location_id", "product_id", "year", "cog"]],
           on=['location_id', 'product_id', 'year'])
ccpy = merge_classifications(ccpy)
ccpy.to_excel("downloads/products_rcpy_country.xlsx", index=False)


cdpy = store['country_department_product_year']\
    .merge(dy, on=['location_id', 'year'])\
    .merge(py, on=['product_id', 'year'])\
    .merge(dpy[["location_id", "product_id", "year", "cog"]],
           on=['location_id', 'product_id', 'year'])
cdpy = merge_classifications(cdpy)
cdpy.to_excel("downloads/products_rcpy_department.xlsx", index=False)


cmpy = store['country_msa_product_year']\
    .merge(my, on=['location_id', 'year'])\
    .merge(py, on=['product_id', 'year'])\
    .merge(ppy[["location_id", "product_id", "year", "cog"]],
           on=['location_id', 'product_id', 'year'])
cmpy = merge_classifications(cmpy)
cmpy.to_excel("downloads/products_rcpy_province.xlsx", index=False)


demographics_department = store['department_year']\
    [["location_id", "year", "gdp_real", "gdp_pc_real", "gdp_nominal", "gdp_pc_nominal", "population"]]\
    .merge(dy, on=['location_id', 'year'])
demographics_department = merge_classifications(demographics_department)
demographics_department.to_excel("downloads/demographics_department.xlsx", index=False)

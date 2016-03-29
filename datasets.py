import pandas as pd
import os.path

from linnaeus import classification

product_classification = classification.load("product/HS/Colombia_Prospedia/out/products_colombia_prospedia.csv")
location_classification = classification.load("location/Peru/datlas/out/locations_peru_datlas.csv")
country_classification = classification.load("location/International/ISO-CID/out/locations_international_iso_cid.csv")


def first(x):
    """Return first element of a group in a pandas GroupBy object"""
    return x.nth(0)


def sum_group(x):
    """Get the sum for a pandas group by"""
    return x.sum()


DATASET_ROOT = "/nfs/home/M/makmanalp/shared_space/cidgrowlab/Atlas/Peru/results/"

def prefix_path(to_prefix):
    return os.path.join(DATASET_ROOT, to_prefix)


def convert_classification(df):

    # Copy in fields and change names appropriately
    new_df = df[["index", "code", "name", "level", "parent_id"]]
    new_df = new_df.rename({
        "index": "id",
        "name": "name_en"
     })

    # Pull in any optional fields
    optional_fields = ["name_es", "name_short_en", "name_short_es",
                       "description_en", "description_es"]

    for field in optional_fields:
        if field in df:
            new_df[field] = df[field]

    return new_df


def hook_country(df):
    df["location"] = "000000"
    return df

trade4digit_country = {
    "read_function": lambda: pd.read_stata(prefix_path("trade_4digit_complexity_country.dta")),
    "hook_pre_merge": hook_country,
    "field_mapping": {
        "hs4": "product",
        "year": "year",
        "fob": "export_value",
        "rca": "export_rca",
        "pci": "pci",
        "eci": "eci",
        "coi": "coi",
        "cog": "cog",
        "density": "density",
    },
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "country"
        },
        "product": {
            "classification": product_classification,
            "level": "4digit"
        },
    },
    "digit_padding": {
        "location": 6,
        "product": 4
    },
    "facet_fields": ["location", "product", "year"],
    "facets": {
        ("location_id", "product_id", "year"): {
            "export_value": first,
            "export_rca": first,
            "coi": first,
            "cog": first,
            "density": first,
        },
        ("product_id", "year"): {
            "export_value": sum_group,
            "pci": first,
        },
        ("location_id", "year"): {
            "export_value": sum_group,
            "eci": first,
        },
    }
}

def hook_department(df):
    df.location = df.location + "0000"
    return df

trade4digit_department = {
    "read_function": lambda: pd.read_stata(prefix_path("trade_4digit_complexity_dpto.dta")),
    "hook_pre_merge": hook_department,
    "field_mapping": {
        "dpto": "location",
        "hs4": "product",
        "year": "year",
        "fob": "export_value",
        "rca": "export_rca",
        "eci": "eci",
        "coi": "coi",
        "cog": "cog",
        "density": "density",
    },
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "department"
        },
        "product": {
            "classification": product_classification,
            "level": "4digit"
        },
    },
    "digit_padding": {
        "location": 6,
        "product": 4
    },
    "facet_fields": ["location", "product", "year"],
    "facets": {
        ("location_id", "product_id", "year"): {
            "export_value": first,
            "export_rca": first,
            "coi": first,
            "cog": first,
            "density": first,
        },
        ("location_id", "year"): {
            "eci": first,
            "export_value": sum_group,
        },
    }
}


def hook_province(df):
    df.location = df.location + "00"
    return df

trade4digit_province = {
    "read_function": lambda: pd.read_stata(prefix_path("trade_4digit_complexity_prov.dta")),
    "hook_pre_merge": hook_province,
    "field_mapping": {
        "prov": "location",
        "hs4": "product",
        "year": "year",
        "fob": "export_value",
        "rca": "export_rca",
        "eci": "eci",
        "coi": "coi",
        "cog": "cog",
        "density": "density",
    },
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "msa"
        },
        "product": {
            "classification": product_classification,
            "level": "4digit"
        },
    },
    "digit_padding": {
        "location": 6,
        "product": 4
    },
    "facet_fields": ["location", "product", "year"],
    "facets": {
        ("location_id", "product_id", "year"): {
            "export_value": first,
            "export_rca": first,
            "coi": first,
            "cog": first,
            "density": first,
        },
        ("location_id", "year"): {
            "eci": first,
        },
    }
}


def hook_rcpy_country(df):
    df["location"] = "000000"
    return df

trade4digit_rcpy_country = {
    "read_function": lambda: pd.read_stata(prefix_path("trade_4digit_rcpy_country.dta")),
    "hook_pre_merge": hook_rcpy_country,
    "field_mapping": {
        "country": "location",
        "cpais": "country",
        "hs4": "product",
        "year": "year",
        "fob": "export_value",
    },
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "country"
        },
        "product": {
            "classification": product_classification,
            "level": "4digit"
        },
        "country": {
            "classification": country_classification,
            "level": "country"
        },
    },
    "digit_padding": {
        "location": 6,
        "product": 4,
    },
    "facet_fields": ["location", "country", "product", "year"],
    "facets": {
        ("country_id", "location_id", "year"): {
            "export_value": sum_group,
        },
        ("product_id", "country_id", "year"): {
            "export_value": sum_group,
        },
    }
}


def hook_demographics(df):
    df.gdp_nominal = df.gdp_nominal * 1000.0
    df.gdp_real = df.gdp_real * 1000.0
    df.location = df.location + "0000"
    return df

demographics = {
    "read_function": lambda: pd.read_stata('/nfs/projects_nobackup/c/cidgrowlab/Atlas/Peru/rawdata/INEI/gdp_pop_department.dta'),
    "hook_pre_merge": hook_demographics,
    "field_mapping": {
        "dpto": "location",
        "year": "year",
        "rgdp": "gdp_real",
        "ngdp": "gdp_nominal",
        "rgdppc": "gdp_pc_real",
        "ngdppc": "gdp_pc_nominal",
        "pop": "population",
    },
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "department"
        }
    },
    "digit_padding": {
        "location": 2,
    },
    "facet_fields": ["location", "year"],
    "facets": {
        ("location_id", "year"): {
            "gdp_real": first,
            "gdp_nominal": first,
            "gdp_pc_real": first,
            "gdp_pc_nominal": first,
            "population": first,
        }
    }
}



if __name__ == "__main__":
    import dataset_tools

    store = pd.HDFStore("data.h5", complib="blosc")

    # Country Product Year
    cpy = dataset_tools.process_dataset(trade4digit_country)
    df = cpy[("location_id", "product_id", "year")].reset_index()

    df.to_hdf(store, "country_product_year", format="table")
    attrs = {
        "sql_table_name": "country_product_year",
        "location_level": "country",
        "product_level": "4digit"
    }
    store.get_storer("/country_product_year").attrs.atlas_metadata = attrs


    # Product Year
    py = cpy[("product_id", "year")].reset_index()
    py.to_hdf(store, "product_year", format="table")
    attrs = {
        "sql_table_name": "product_year",
        "product_level": "4digit",
    }
    store.get_storer("product_year").attrs.atlas_metadata = attrs


    df = dataset_tools.process_dataset(trade4digit_department)

    # Department Product Year
    dpy = df[("location_id", "product_id", "year")].reset_index()
    dpy.to_hdf(store, "department_product_year", format="table")
    attrs = {
        "sql_table_name": "department_product_year",
        "location_level": "department",
        "product_level": "4digit"
    }
    store.get_storer("department_product_year").attrs.atlas_metadata = attrs

    # Department Year
    trade_dpy = df[("location_id", "year")]

    demographics = dataset_tools.process_dataset(demographics)
    demographics = demographics[("location_id", "year")]

    dpy = trade_dpy\
        .join(demographics, how="outer")\
        .reset_index()

    dpy = dpy.drop("export_value", axis=1)

    dpy.to_hdf(store, "department_year", format="table")
    attrs = {
        "sql_table_name": "department_year",
        "location_level": "department",
    }
    store.get_storer("department_year").attrs.atlas_metadata = attrs


    # Province
    df = dataset_tools.process_dataset(trade4digit_province)

    # Province Product Year
    ppy = df[("location_id", "product_id", "year")].reset_index()

    ppy.to_hdf(store, "msa_product_year", format="table")
    attrs = {
        "sql_table_name": "msa_product_year",
        "location_level": "msa",
        "product_level": "4digit"
    }
    store.get_storer("msa_product_year").attrs.atlas_metadata = attrs

    # Province Year
    py = df[("location_id", "year")].reset_index()

    py.to_hdf(store, "msa_year", format="table")
    attrs = {
        "sql_table_name": "msa_year",
        "location_level": "msa",
    }
    store.get_storer("msa_year").attrs.atlas_metadata = attrs


    # RCPY Country
    df = dataset_tools.process_dataset(trade4digit_rcpy_country)

    ret = df[("country_id", "location_id", "year")].reset_index()
    ret.to_hdf(store, "country_country_year", format="table")
    attrs = {
        "sql_table_name": "country_country_year",
        "location_level": "country",
        "country_level": "country",
    }
    store.get_storer("country_country_year").attrs.atlas_metadata = attrs

    ret = df[("product_id", "country_id", "year")].reset_index()
    ret.to_hdf(store, "partner_product_year", format="table")
    attrs = {
        "sql_table_name": "partner_product_year",
        "country_level": "country",
        "product_level": "4digit"
    }
    store.get_storer("partner_product_year").attrs.atlas_metadata = attrs


    # Product Classification
    df = product_classification.table.reset_index()

    df.to_hdf(store, "/classifications/product", format="table")
    attrs = {
        "sql_table_name": "product",
    }
    store.get_storer("/classifications/product").attrs.atlas_metadata = attrs


    # Location Classification
    df = location_classification.table.reset_index()

    df.to_hdf(store, "/classifications/location", format="table")
    attrs = {
        "sql_table_name": "location",
    }
    store.get_storer("/classifications/location").attrs.atlas_metadata = attrs

    store.close()


import pandas as pd
import os.path

from linnaeus import classification

product_classification = classification.load("product/HS/Colombia_Prospedia/out/products_colombia_prospedia.csv")
location_classification = classification.load("location/Peru/datlas/out/locations_peru_datlas.csv")


def first(x):
    """Return first element of a group in a pandas GroupBy object"""
    return x.nth(0)


def sum_group(x):
    """Get the sum for a pandas group by"""
    return x.sum()


DATASET_ROOT = "/nfs/home/M/makmanalp/shared_space/cidgrowlab/Atlas/Peru/results/"

def prefix_path(to_prefix):
    return os.path.join(DATASET_ROOT, to_prefix)


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
        }
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
        },
        ("location_id", "year"): {
            "export_value": sum_group,
        },
        ("product_id", "year"): {
            "export_value": sum_group,
        }
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
        }
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
        }
    }
}



if __name__ == "__main__":
    import dataset_tools

    store = pd.HDFStore("data.h5", complib="blosc")

    # Country Product Year
    df = dataset_tools.process_dataset(trade4digit_country)
    df = df[("location_id", "product_id", "year")].reset_index()

    df.to_hdf(store, "country_product_year", format="table")
    attrs = {
        "sql_table_name": "country_product_year",
        "location_level": "country",
        "product_level": "4digit"
    }
    store.get_storer("/country_product_year").attrs.atlas_metadata = attrs

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
    dpy = df[("location_id", "year")].reset_index()
    dpy.to_hdf(store, "department_year", format="table")
    attrs = {
        "sql_table_name": "department_year",
        "location_level": "department",
    }
    store.get_storer("department_year").attrs.atlas_metadata = attrs

    # Product Year
    dpy = df[("product_id", "year")].reset_index()
    dpy.to_hdf(store, "product_year", format="table")
    attrs = {
        "sql_table_name": "product_year",
        "product_level": "4digit",
    }
    store.get_storer("product_year").attrs.atlas_metadata = attrs


    # Province Product Year
    df = dataset_tools.process_dataset(trade4digit_province)
    df = df[("location_id", "product_id", "year")].reset_index()

    df.to_hdf(store, "msa_product_year", format="table")
    attrs = {
        "sql_table_name": "msa_product_year",
        "location_level": "msa",
        "product_level": "4digit"
    }
    store.get_storer("msa_product_year").attrs.atlas_metadata = attrs


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


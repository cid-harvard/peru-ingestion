import pandas as pd
import os.path

from linnaeus import classification

product_classification = classification.load("product/HS/Colombia_Prospedia/out/products_colombia_prospedia.csv")
location_classification = classification.load("location/International/Mexico/out/locations_international_mexico.csv")


def first(x):
    """Return first element of a group in a pandas GroupBy object"""
    return x.nth(0)


def sum_group(x):
    """Get the sum for a pandas group by"""
    return x.sum()


DATASET_ROOT = "/Users/makmana/cepii/new/"

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


hs92_4digit_country = {
    "read_function": lambda: pd.read_stata(prefix_path("./H0_cpy_all.dta")),
    "field_mapping": {
        "exporter": "location",
        "commoditycode": "product",
        "year": "year",
        "export_value": "export_value",
        "import_value": "import_value",
        "rca": "export_rca",
        "distance": "distance",
        #"diversity": "diversity",
        "eci": "eci",
        "pci": "pci",
        "oppgain": "cog",
        "oppval": "coi",
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
        "product": 4
    },
    "facet_fields": ["location", "product", "year"],
    "facets": {
        ("location_id", "product_id", "year"): {
            "export_value": first,
            "import_value": first,
            "export_rca": first,
            "cog": first,
            "distance": first,
        },
        ("location_id", "year"): {
            "coi": first,
            "eci": first,
            #"diversity": first,
            "export_value": sum_group,
            "import_value": sum_group,
        },
        ("product_id", "year"): {
            "pci": first,
            "export_value": sum_group,
            "import_value": sum_group,
        },
    }
}



hs92_4digit_country_partner = {
    "read_function": lambda: pd.read_stata(prefix_path("./H0_2015.dta")),
    "field_mapping": {
        "exporter": "location",
        "importer": "partner",
        "commoditycode": "product",
        "year": "year",
        "export_value": "export_value",
        "import_value": "import_value",
        #"value_exporter": "value_exporter",
        #"case": "case",
    },
    "classification_fields": {
        "location": {
            "classification": location_classification,
            "level": "country"
        },
        "partner": {
            "classification": location_classification,
            "level": "country"
        },
        "product": {
            "classification": product_classification,
            "level": "4digit"
        },
    },
    "digit_padding": {
        "product": 4
    },
    "facet_fields": ["location", "partner", "product", "year"],
    "facets": {
        ("location_id", "partner_id", "product_id", "year"): {
            "export_value": first,
            "import_value": first,
        },
        ("location_id", "partner_id", "year"): {
            "export_value": sum_group,
            "import_value": sum_group,
        },
    }
}


if __name__ == "__main__":
    import dataset_tools

    store = pd.HDFStore("data.h5", complib="blosc")

    # Product Classification
    df = product_classification.table.reset_index()

    df.to_hdf(store, "/classifications/product", format="table")
    attrs = {
        "sql_table_name": "product",
    }
    store.get_storer("/classifications/product").attrs.atlas_metadata = attrs


    # Country Classification
    df = location_classification.table.reset_index()

    df.to_hdf(store, "/classifications/location", format="table")
    attrs = {
        "sql_table_name": "location",
    }
    store.get_storer("/classifications/location").attrs.atlas_metadata = attrs

    # Country Product Year
    cpy = dataset_tools.process_dataset(hs92_4digit_country)
    df = cpy[("location_id", "product_id", "year")].reset_index()

    df.to_hdf(store, "country_product_year", format="table")
    attrs = {
        "sql_table_name": "country_product_year",
        "location_level": "country",
        "product_level": "4digit",
        "levels":{
            "product": "4digit",
            "location": "country"
        },
    }
    store.get_storer("/country_product_year").attrs.atlas_metadata = attrs


    # Country Year
    cy = cpy[("location_id", "year")].reset_index()

    cy.to_hdf(store, "country_year", format="table")
    attrs = {
        "sql_table_name": "country_year",
        "levels":{
            "location": "country"
        },
    }
    store.get_storer("country_year").attrs.atlas_metadata = attrs


    # Product Year
    py = cpy[("product_id", "year")].reset_index()
    py.to_hdf(store, "product_year", format="table")
    attrs = {
        "sql_table_name": "product_year",
        "levels":{
            "product": "4digit",
        },
    }
    store.get_storer("product_year").attrs.atlas_metadata = attrs

    # Country Partner Product Year
    cppy = dataset_tools.process_dataset(hs92_4digit_country_partner)
    df = cppy[("location_id", "partner_id", "product_id", "year")].reset_index()

    df.to_hdf(store, "country_partner_product_year", format="table")
    attrs = {
        "sql_table_name": "country_partner_product_year",
        "levels":{
            "product": "4digit",
            "location": "country",
            "partner": "country"
        },
    }
    store.get_storer("/country_partner_product_year").attrs.atlas_metadata = attrs


    df.to_hdf(store, "country_partner_year", format="table")
    attrs = {
        "sql_table_name": "country_partner_year",
        "levels":{
            "location": "country",
            "partner": "country"
        },
    }
    store.get_storer("/country_partner_year").attrs.atlas_metadata = attrs

    store.close()


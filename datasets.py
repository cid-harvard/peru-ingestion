import pandas as pd
import os.path

from linnaeus import classification

product_classification = classification.load("product/HS/Colombia_Prospedia/out/products_colombia_prospedia.csv")
location_classification = classification.load("location/Colombia/Prospedia/out/locations_colombia_prosperia.csv")
industry_classification = classification.load("industry/ISIC/Colombia_Prosperia/out/industries_colombia_isic_prosperia.csv")
country_classification = classification.load("location/International/DANE/out/locations_international_dane.csv")
occupation_classification = classification.load("occupation/SOC/Colombia/out/occupations_soc_2010.csv")


country_classification.table.code = country_classification.table.code.astype(str).str.zfill(3)


def first(x):
    """Return first element of a group in a pandas GroupBy object"""
    return x.nth(0)


def sum_group(x):
    """Get the sum for a pandas group by"""
    return x.sum()


DATASET_ROOT = "/nfs/home/M/makmanalp/shared_space/cidgrowlab/Atlas/Peru/results/"
YEAR_MIN_TRADE = 2007
YEAR_MAX_TRADE = 2014


def prefix_path(to_prefix):
    return os.path.join(DATASET_ROOT, to_prefix)


def load_trade4digit_country():
    prescriptives = pd.read_stata(prefix_path("Trade/exp_ecomplexity_rc.dta"))

    exports = pd.read_stata(prefix_path("Trade/exp_rpy_rc_p4.dta"))
    exports = exports.rename(columns={"X_rpy_d": "export_value",
                                      "NP_rpy": "export_num_plants"})
    imports = pd.read_stata(prefix_path("Trade/imp_rpy_rc_p4.dta"))
    imports = imports.rename(columns={"X_rpy_d": "import_value",
                                      "NP_rpy": "import_num_plants"})

    descriptives = exports.merge(imports, on=["yr", "r", "p"], how="outer")
    descriptives = descriptives.fillna({
        "export_value": 0,
        "export_num_plants": 0,
        "import_value": 0,
        "import_num_plants": 0,
    })

    combo = prescriptives.merge(descriptives,
                                left_on=["yr", "r", "p4"],
                                right_on=["yr", "r", "p"])

    combo = combo[combo.yr.between(YEAR_MIN_TRADE, YEAR_MAX_TRADE)]
    combo["r"] = "COL"
    return combo

trade4digit_country = {
    "read_function": lambda: pd.read_stata(prefix_path("trade_4digit_department.dta")),
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
        "location": 2,
        "product": 4
    },
    "facet_fields": ["location", "product", "year"],
    "facets": {
        ("location_id", "product_id", "year"): {
            "export_value": first,
        }
    }
}


if __name__ == "__main__":
    import dataset_tools
    df = dataset_tools.process_dataset(trade4digit_country)
    df = df[("location_id", "product_id", "year")].reset_index()
    df = df.rename(columns={"location_id": "department_id"})

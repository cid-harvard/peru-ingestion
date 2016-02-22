from datasets import trade4digit_country
import dataset_tools

if __name__ == "__main__":
    df = dataset_tools.process_dataset(trade4digit_country)
    df = df[("location_id", "product_id", "year")].reset_index()
    df = df.rename(columns={"location_id": "department_id"})

    df["year"] = df["year"].astype(int)
    df["export_value"] = df["export_value"].astype(int)
    df["import_value"] = df["export_value"]

    df["export_num_plants"] = 1
    df["import_num_plants"] = 1

    df["distance"] = 0.5
    df["export_rca"] = 1
    df["cog"] = 0
    df["coi"] = 0

    # Lima
    df[df.department_id == 5].to_json("./lima.json", orient="records")

    # Cusco
    df[df.department_id == 2].to_json("./cusco.json", orient="records")

    # Ucayali
    df[df.department_id == 11].to_json("./ucayali.json", orient="records")

    # La Libertad
    df[df.department_id == 4].to_json("./lalibertad.json", orient="records")

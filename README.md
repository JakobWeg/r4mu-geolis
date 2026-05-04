# r4mu-geolis

Verortung von Ladepunkten und Ladevorgängen in R4MU

Paper auf [Sharepoint](https://rlinstitutde.sharepoint.com/:b:/s/374_Retail4Multi-Use-374_internal_Team/IQCfIs4REkcFRYHm0vmd1rcNARQQxrlGiFq9GOj90zq6G7M?e=ibbZXc)

## Run

1. Env erstellen mit `conda env create -f environment.yml` und aktivieren mit
   `conda activate r4mu-geolis`
2. Daten vom [Sharepoint](https://rlinstitutde.sharepoint.com/:f:/s/374_Retail4Multi-Use-374_internal_Team/IgCDUBLoOBELRossBuDwwGVsAToVfW3t5gZ9KFJFuvXuask?e=T4dOeu)
  in [data](data) legen, Ausnahme: Folgende Ordner in [scenario](scenario)
  legen:
   - [Ladeprofile_Privatverkehr_parquet](scenario/Ladeprofile_Privatverkehr_parquet)
   - [Ladeprofile_Wirtschaftsverkehr_parquet](scenario/Ladeprofile_Wirtschaftsverkehr_parquet)
3. Ausführen für 2035 - Szenarieneinstellungen in
   [config.cfg](scenario/config.cfg):
   - Jahr in Filenames auf 2035 setzen
   - nacheinander Szenarien ausführen mit `python __main__.py`:

1_ref_2035:
- `[use_cases]`: Alle UC auf true
- multi_use_concept = false
- flexibility_multi_use = 0
2_mehrfachnutzung_2035:
- `[use_cases]`: Nur public und retail auf true
- multi_use_concept = true
- flexibility_multi_use = 0
3_mehrfachnutzung_flex_2035:
- `[use_cases]`: Nur public und retail auf true
- multi_use_concept = true
- flexibility_multi_use = 48

4. Dateien aus [results](results) zusammenführen: Dateien aus erstem Run
   (`1_ref_2035`) ohne `retail` und `public` in andere runs kopieren

## Results

Liegen [hier](https://rlinstitutde.sharepoint.com/:f:/s/374_Retail4Multi-Use-374_internal_Team/IgBShoLqZtL7Trzl8JAcVzTUAVbcBJg9LvzvUu7KiIADPDU?e=IENtAT)

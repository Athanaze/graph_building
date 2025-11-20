rm logs/*
rm CSVs/data_filtered_citations_changed.csv
python preprocess_citations.py
./target/release/cartesian-law-analysis
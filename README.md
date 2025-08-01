# Publication-Trend-Analysis

Proyek ini bertujuan untuk menganalisis tren publikasi dosen berdasarkan data dari dua sumber utama: **SCOPUS** dan **SISTER**. Data dari masing-masing sumber dibersihkan, dilakukan penyamaan formatnya, dan digabungkan berdasarkan NIP dan ID Scopus untuk keperluan analisis lebih lanjut.

Fitur proyek saat ini:

    - Pembersihan data publikasi dari Scopus dan SISTER.

    - Normalisasi format nama, ID, dan kolom-kolom lainnya.

    - Penggabungan berdasarkan `nip` dan `id_scopus`.

    - Penggabungan menggunakan Fuzzy Matching.

    - Menangani nilai kosong agar tidak ditampilkan sebagai `NaN`.

## Struktur Proyek

```
.
├── data/                               # Data storage
│   ├── cleaned/                        # Preprocessed and cleaned data
│   └── raw/                            # Original data
│
├── src/                                # Source code
│   ├── data-cleaning/                  # Data preprocessing
│   │   └── combine.py                  # Data merge functions
│   │   └── preprocessing_scopus.py     # Preprocessing functions
│   │   └── preprocessing_scopus.py     # Preprocessing functions
│   ├── exploratory/                    # Exploratory data analysis
│   │   └── EDA_scopus.ipynb            # Jupyter notebook for EDA
│   │   └── EDA_sister.ipynb            # Jupyter notebook for EDA
│   └── modelling/                      # Modelling implementation
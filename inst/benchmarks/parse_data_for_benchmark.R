library(data.table)
library(stringplus)
set_string_ops("&", "|")

# data path to store large benchmark files
DATA_PATH <- "~/datasets"


################################################################################
# GAIA celestial pseudocolor dataset
# Data used for pseudocolor plot inst/benchmarks/GAIA_galaxy_pseudocolor.png
files <- c("http://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source_with_rv/csv/GaiaSource_1584380076484244352_2200921635402776448.csv.gz",
           "http://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source_with_rv/csv/GaiaSource_2200921875920933120_3650804325670415744.csv.gz",
           "http://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source_with_rv/csv/GaiaSource_2851858288640_1584379458008952960.csv.gz",
           "http://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source_with_rv/csv/GaiaSource_3650805523966057472_4475721411269270528.csv.gz",
           "http://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source_with_rv/csv/GaiaSource_4475722064104327936_5502601461277677696.csv.gz",
           "http://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source_with_rv/csv/GaiaSource_5502601873595430784_5933051501826387072.csv.gz",
           "http://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source_with_rv/csv/GaiaSource_5933051914143228928_6714230117939284352.csv.gz",
           "http://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source_with_rv/csv/GaiaSource_6714230465835878784_6917528443525529728.csv.gz")

stars <- lapply(files, function(f) {
  print(f)
  data.table::fread(f, select = c("parallax", "dec", "ra", "astrometric_pseudo_colour", "lum_val"), colClasses = "character")
}) %>% rbindlist %>% as.data.frame
fwrite(stars, file = DATA_PATH & "/GAIA_pseudocolor.csv.gz", sep = ",")

# enwik8, a single column dataframe of first 1e8 lines in wikipedia
download_path <- DATA_PATH & "enwik8.zip"
download.file("http://mattmahoney.net/dc/enwik8.zip", download_path)
con <- unz(download_path, "enwik8")
data <- data.frame(text=readLines(con))
close(con)
fwrite(data, DATA_PATH & "/enwik8.csv.gz")

################################################################################
# T-cell data
# https://clients.adaptivebiotech.com/pub/covid-2020
# wget to DATA_PATH
dl_file <- "https://adaptivepublic.blob.core.windows.net/publishedproject-supplements/covid-2020/ImmuneCODE-Repertoires-002.2.tgz"
local_file <- DATA_PATH & "/ImmuneCODE-Repertoires-002.2.tgz"
system("wget %s -O %s" | c(dl_file, local_file))
# untar to DATA_PATH
system( "tar -xvf %s" | local_file )
cols <- c(rearrangement = "character", extended_rearrangement = "character", 
bio_identity = "character", amino_acid = "character", templates = "integer", 
frame_type = "character", rearrangement_type = "character", productive_frequency = "numeric", 
cdr1_start_index = "integer", cdr1_rearrangement_length = "integer", 
cdr2_start_index = "integer", cdr2_rearrangement_length = "integer", 
cdr3_start_index = "integer", cdr3_length = "integer", v_index = "integer", 
n1_index = "integer", d_index = "integer", n2_index = "integer", 
j_index = "integer", v_deletions = "integer", n2_insertions = "integer", 
d3_deletions = "integer", d5_deletions = "integer", n1_insertions = "integer", 
j_deletions = "integer", chosen_j_allele = "character", chosen_j_family = "character", 
chosen_j_gene = "character", chosen_v_allele = "character", chosen_v_family = "character", 
chosen_v_gene = "character", d_allele = "character", d_allele_ties = "character", 
d_family = "character", d_family_ties = "character", d_gene = "character", 
d_gene_ties = "character", d_resolved = "character", j_allele = "character", 
j_allele_ties = "character", j_family = "character", j_family_ties = "character", 
j_gene = "character", j_gene_ties = "character", j_resolved = "character", 
v_allele = "character", v_allele_ties = "character", v_family = "character", 
v_family_ties = "character", v_gene = "character", v_gene_ties = "character", 
v_resolved = "character")
data <- fread(DATA_PATH & "/ImmuneCODE-Repertoires-002.2/ADIRP0000010_20200612_Frblood_Repertorie_TCRB.tsv", colClasses = cols, na.strings = c("no data", "unknown", "na"))
data <- as.data.frame(data)
saveRDS(data, file = DATA_PATH & "/T_cell_ADIRP0000010.rds")


################################################################################
library(dslabs)
data <- dslabs::read_mnist()
saveRDS(data, file = DATA_PATH & "/dslabs_mnist.rds")

################################################################################
library(recount3)

z <- recount3::create_rse_manual(
  project = "HEART",
  project_home = "data_sources/gtex",
  organism = "human",
  annotation = "gencode_v26",
  type = "gene"
)
counts <- recount3::transform_counts(z)
mode(counts) <- "integer"
saveRDS(counts, file = DATA_PATH & "/recount3_gtex_heart.rds")


################################################################################
# https://www.kaggle.com/datasets/datadetective08/washington-d-c-housing-market-2024
# DC_real_estate_June_2024.json.gz

################################################################################
# https://www.kaggle.com/datasets/eren2222/nasdaq-nyse-nyse-a-otc-daily-stock-1962-2024
# NYSE_1962_2024.csv.gz

################################################################################
# ERA5 wind data
library(ecmwfr)
library(ncdf4)
library(askpass)
# register https://cds.climate.copernicus.eu/ for API key

user <- askpass("user")
key <- askpass("key")
wf_set_key(user=user, key = key, service = "cds")

request <- list(
  "dataset_short_name" = 'reanalysis-era5-land-monthly-means',
  "product_type" = "reanalysis",
  "variable" = c('10m_u_component_of_wind', '10m_v_component_of_wind'),
  'month' = 1:12,
  'year' = '2023',
  "format" = "netcdf",
  "target" = "reanalysis-era5-land-wind-monthly.nc"
)


file <- wf_request(
  user     = user,
  request  = request,
  transfer = TRUE,
  path     = DATA_PATH)

nc <- nc_open(DATA_PATH & "/reanalysis-era5-land-wind-monthly.nc")
data <- list(u10 = ncvar_get(nc, "u10"), v10 = ncvar_get(nc, "u10"))
saveRDS(data, file = DATA_PATH & "/era5_land_wind_20230101.rds")

################################################################################
# Berkeley 2010-2019 global temperature
library(ncdf4)
nc <- nc_open(DATA_PATH & "/Berkeley_grid_temp_2010.nc")
data <- ncvar_get(nc, "temperature")
saveRDS(data, file = DATA_PATH & "/Berkeley_grid_temp_2010.rds")

################################################################################
# Steam game database 2024
# https://www.kaggle.com/datasets/artermiloff/steam-games-dataset

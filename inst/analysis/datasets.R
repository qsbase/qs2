library(data.table)
library(stringplus)
set_string_ops("&", "|")

# data path to store large benchmark files
DATA_PATH <- "~/datasets/processed"

################################################################################
# GAIA celestial pseudocolor dataset
# License: Other (https://www.cosmos.esa.int/web/gaia-users/license)
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
# License: CC BY 4.0
# Reference: https://clients.adaptivebiotech.com/pub/covid-2020
# Nolan, Sean, et al. "A large-scale database of T-cell receptor beta (TCRβ) sequences and binding associations from natural and synthetic exposure to SARS-CoV-2." Research square (2020).

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
# MNIST
# License: Artistic-2.0
# Reference: https://cran.r-project.org/web/packages/dslabs/index.html

library(dslabs)
data <- dslabs::read_mnist()
saveRDS(data, file = DATA_PATH & "/dslabs_mnist.rds")

################################################################################
# RNA-Seq gtex heart data
# License: Artistic-2.0
# Reference: https://bioconductor.org/packages/release/bioc/html/recount3.html

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
# EU Copernicus ERA5 wind data
# License: other (https://www.copernicus.eu/en/access-data/copyright-and-licences)
# Reference: https://cds.climate.copernicus.eu/
# register for API key


library(ecmwfr)
library(ncdf4)
library(askpass)

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
# License: Creative Commons BY-NC 4.0 International
# Reference: https://berkeleyearth.org/data/

library(ncdf4)
nc <- nc_open(DATA_PATH & "/Berkeley_grid_temp_2010.nc")
data <- ncvar_get(nc, "temperature")
saveRDS(data, file = DATA_PATH & "/Berkeley_grid_temp_2010.rds")

################################################################################
# Open street map
# License: Open Data Commons Open Database License (ODbL)
# Reference: https://wiki.openstreetmap.org/wiki/Downloading_data
# https://overpass-api.de/api/map?bbox=-122.8938,47.3588,-121.7903,47.8556

library(xml2)
data <- read_xml(DATA_PATH & "/Oahu_OSM.xml")
data <- as_list(data)
saveRDS(data, file = DATA_PATH & "Oahu_OSM.rds")


################################################################################
# NYC vehicle collisions
# License: "This dataset is intended for public access and use."
# https://catalog.data.gov/dataset/motor-vehicle-collisions-crashes

################################################################################
# methylation volcano
# License: 	Artistic-2.0
# Reference: https://bioconductor.org/packages/release/workflows/html/methylationArrayAnalysis.html

library(minfi)
library(methylationArrayAnalysis)

dataDirectory <- system.file("extdata", package = "methylationArrayAnalysis")
targets <- read.metharray.sheet(dataDirectory, pattern="SampleSheet.csv")
rgSet <- read.metharray.exp(targets=targets)
data <- list(red = getRed(rgSet), green = getGreen(rgSet))

saveRDS(DMPs, file = DATA_PATH & "/methylation_450k.rds")

################################################################################
# clifford attractor
# License: N/A
# Reference: https://paulbourke.net/fractals/clifford/

library(glow)

N <- 2e7
x0 <- 0.1
y0 <- 0
cliff_points <- clifford_attractor(N, 1.886,-2.357,-0.328, 0.918, x0, y0)
saveRDS(cliff_points, file = DATA_PATH & "/clifford_100M.rds")

################################################################################
# 1000 genomes non-coding VCF
# https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase1/analysis_results/functional_annotation/annotated_vcfs/ALL.wgs.integrated_phase1_release_v3_noncoding_annotation_20120330.20101123.snps_indels_sv.sites.vcf.gz
# 1000genomes_noncoding.vcf.gz
# License: creative commons Attribution-NonCommercial-ShareAlike 3.0 Unported licence
# Reference: A global reference for human genetic variation, The 1000 Genomes Project Consortium, Nature 526, 68-74 (01 October 2015) doi:10.1038/nature15393.
# https://www.internationalgenome.org/faq/how-do-I-cite-IGSR

# fix broken VCF format
data <- readLines(DATA_PATH & "/1000genomes_noncoding.vcf.gz")
data <-  grep("^##", data, value=T, invert=T)
data[1] <- gsub("^#", "", data[1])
data[1] <- gsub("\\W+", "\t", data[1])
data <- fread(text = data, sep = "\t", data.table=F)
fwrite(data, file = DATA_PATH & "/1000genomes_noncoding_vcf.csv.gz", sep = ",")

################################################################################
# B-cell AIRR data "Pet shop mouse 3"
# License: N/A
# References:
# Corrie et al. iReceptor: a platform for querying and analyzing antibody/B-cell and T-cell receptor repertoire data across federated repositories, Immunological Reviews, Volume 284:24-41 (2018).
# Greiff, Victor, et al. "Systems analysis reveals high genetic and antigen-driven predetermination of antibody repertoires throughout B cell development." Cell reports 19.7 (2017): 1467-1478.
# https://www.ebi.ac.uk/ena/browser/view/PRJEB18631

################################################################################
# kaggle datasets

# Twitter Sentiment140
# https://www.kaggle.com/datasets/kazanova/sentiment140
# twitter_sentiment140.csv.gz
# License: N/A
# Reference: Go, A., Bhayani, R. and Huang, L., 2009. Twitter sentiment classification using distant supervision. CS224N Project Report, Stanford, 1(2009), p.12.
# 

# Steam game database 2024
# https://www.kaggle.com/datasets/artermiloff/steam-games-dataset
# steam_games_2024.csv.gz
# License: MIT

# protein structure PISCES
# https://www.kaggle.com/datasets/alfrandom/protein-secondary-structure
# pisces_2018.csv.gz
# License: Database Contents License (DbCL) v1.0
# Reference: Wang, Guoli, and Roland L. Dunbrack Jr. "PISCES: a protein sequence culling server." Bioinformatics 19.12 (2003): 1589-1591.

# IP location dataset
# https://www.kaggle.com/datasets/joebeachcapital/global-ip-dataset-by-location-2023
# ip_location_2023.csv.gz
# License: CC BY 4.0

# DC real estate listings 2024
# https://www.kaggle.com/datasets/datadetective08/washington-d-c-housing-market-2024
# DC_real_estate_2024.json.gz
# License: CC BY-NC-SA 4.0

# NYSE stock prices 1962 to 2024
# https://www.kaggle.com/datasets/eren2222/nasdaq-nyse-nyse-a-otc-daily-stock-1962-2024
# NYSE_1962_2024.csv.gz
# License: Apache 2.0

# Netflix movie ratings
# https://www.kaggle.com/datasets/rishitjavia/netflix-movie-rating-dataset
# Netflix_Ratings.csv.gz
# License: CC0: Public Domain


library(data.table)
library(stringplus)
set_string_ops("&", "|")

# data path to store large benchmark files
DATA_PATH <- "/mnt/n/R_stuff"

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




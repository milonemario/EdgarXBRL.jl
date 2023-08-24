module Downloader

using Dates
using ZipFile
using CSV
using DataFrames
using Parquet2: Dataset, writefile
using HTTP
using EzXML
using ProgressBars

import Logging
Logging.disable_logging(Logging.Warn)


include("./Paths.jl")
using .Paths

include("./Xbrl.jl")
using .Xbrl

export download_files
export download_data

function download_file(cik::Int, filing_id::String, filename::String)
    path = Paths.filing_path(cik, filing_id)
    file = path * filename
    if !isfile(file)
        if !isdir(path)
            mkpath(path)
        end
        url = Paths.filing_url(cik, filing_id)
        file_url = url * filename
        write(file, HTTP.get(file_url).body)
        sleep(rand() + 1.)
    end
    file
end

function download_files(cik::Int, filing_id::String)
    # Main index file
    download_file(cik, filing_id, Paths.filing_index_filename(filing_id))
    # Instance
    download_file(cik, filing_id, Xbrl.xbrl_instance(cik, filing_id))
    # Schemas
    download_file.(cik, filing_id, Xbrl.xbrl_schemas(cik, filing_id))
    # Calculations
    download_file.(cik, filing_id, Xbrl.xbrl_calculations(cik, filing_id))
end

function download_data(df_index::DataFrame)
    for row in ProgressBar(eachrow(df_index))
        download_files(row.CIK, row.filing_id)
    end
end

function download_data(form::String, year::Int, quarter::Int)
    df_index = DataFrame(Dataset("index/index.parquet"))
    df_index = filter(Symbol("Form Type") => x -> x.==form, df_index)
    df_index = filter(:year => x -> x.==year, df_index)
    df_index = filter(:quarter => x -> x.==quarter, df_index)
    download_data(df_index)
end

function download_data(form::String, year::Int)
    df_index = DataFrame(Dataset("index/index.parquet"))
    df_index = filter(Symbol("Form Type") => x -> x.==form, df_index)
    df_index = filter(:year => x -> x.==year, df_index)
    download_data(df_index)
end

end # module

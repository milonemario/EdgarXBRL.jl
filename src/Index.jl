module Index

using ZipFile
using HTTP
using DataFrames
using Dates
using Parquet2: writefile
using CSV

export update_index

function index_url(year::Int, quarter::Int)
    "https://www.sec.gov/Archives/edgar/full-index/" * string(year) * "/QTR" * string(quarter) * "/"
end

function index_path(year::Int, quarter::Int)
    "index/" * string(year) * "/QTR" * string(quarter) * "/"
end

function download_index(year::Int, quarter::Int)
    path = index_path(year, quarter)
    if !isdir(path)
        file = path * "xbrl.zip"
        url = index_url(year, quarter) * "xbrl.zip"
        mkpath(path)
        write(file, HTTP.get(url).body)
        sleep(rand() + .1)
    end
    
end

function update_index()

    function get_filing_id(filename::String)
        txt = split(filename, "/")[4]
        string(split(txt, ".")[1])
    end

    function add_index(df_index, y, q)
        print("Index for ", string(y), " quarter ", string(q), "\u1b[K\r")
        download_index(y, q)
        zfile = index_path(y, q) * "xbrl.zip"
        zarchive = ZipFile.Reader(zfile)
        for f in zarchive.files
            if f.name == "xbrl.idx"
                file = read(f, String)
                lines = split(file, "\n")
                lines_data = [lines[9], lines[11:end]...]
                data = join(lines_data, "\n")
                df = CSV.File(IOBuffer(data); delim="|") |> DataFrame
                df.year .= y
                df.quarter .= q
                df.filing_id .= get_filing_id.(df.Filename)
                if nrow(df) > 0
                    df_index = vcat(df_index, df)
                end
            else
                @error "Problem when extracting index for " * string(y) * " quarter " * string(q)
            end
        end
        close(zarchive)
        df_index
    end

    df_index = DataFrame()
    current_year = year(today())
    for y in 1993:current_year-1
        for q in 1:4
            df_index = add_index(df_index, y, q)
        end
    end
    writefile("index/index.parquet", df_index)

end

end # module

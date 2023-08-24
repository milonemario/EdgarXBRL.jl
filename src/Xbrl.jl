module Xbrl

using DataFrames
using Dates
using EzXML

import Logging
Logging.disable_logging(Logging.Warn)

include("./Paths.jl")
using .Paths

function data_files(cik::Int, filing_id::String)
    path = Paths.filing_path(cik, filing_id)
    filename = Paths.filing_index_filename(filing_id)
    file = path * filename
    doc_index = EzXML.readhtml(file)

    tables_index = findall("//table", doc_index)
    table_files = [t for t in tables_index if t["summary"]=="Data Files"]
    if length(table_files) == 0
        @error "Cannot retrieve files table for filing " * filing_id
    else
        table_files = table_files[1]
        table = elements(table_files)
        header = table[1]
        rows = table[2:end]
        col_names = []
        for col_name in elements(header)
            push!(col_names, col_name.content)
        end
        files = []
        for row in rows
            file = Dict()
            cols = elements(row)
            for i_col in eachindex(cols)
                file[col_names[i_col]] = cols[i_col].content
            end
            push!(files, file)
        end
        df_files = vcat(DataFrame.(files)...)
    end
    df_files
end

function xbrl_files(cik::Int, filing_id::String, contains::String)
    df_files = data_files(cik, filing_id)
    df_contains = filter([:Description, :Type] => (x, y) -> occursin.(contains, x) .|| occursin.(contains, y), df_files)
    path = Paths.filing_path(cik, filing_id)
    [d for d in df_contains.Document]
end

function xbrl_instance(cik::Int, filing_id::String)
    files = xbrl_files(cik, filing_id, "INS")
    if length(files) > 1
        @error "More than one instance file not supported."
    end
    files[1]
end

function xbrl_schemas(cik::Int, filing_id::String)
    xbrl_files(cik, filing_id, "SCH")
end

function xbrl_calculations(cik::Int, filing_id::String)
    xbrl_files(cik, filing_id, "CAL")
end

function xbrl_labels(cik::Int, filing_id::String)
    xbrl_files(cik, filing_id, "LAB")
end

function xbrl_definitions(cik::Int, filing_id::String)
    xbrl_files(cik, filing_id, "DEF")
end

function xbrl_presentations(cik::Int, filing_id::String)
    xbrl_files(cik, filing_id, "PRE")
end

function context_data(cik::Int, filing_id::String)
    path = Paths.filing_path(cik, filing_id)
    file = path * xbrl_instance(cik, filing_id) 
    doc = EzXML.readxml(file)

    context_nodes = [n for n in elements(doc.root) if n.name=="context"]

    df_context = DataFrame(
        context = String[],
        startdate = Union{Missing, Date}[],
        enddate = Union{Missing, Date}[],
        instant = Union{Missing, Date}[]
    )

    for c in context_nodes
        context = c["id"]
        periods = [n for n in elements(c) if n.name=="period"]
        if length(periods) > 1
            @error "Multiple periods in context."
        else
            period = periods[1]
        end
        instant = startdate = enddate = missing
        for e in elements(period)
            if e.name == "instant"
                instant = parse(Date, e.content)
            elseif e.name == "startDate"
                startdate= parse(Date, e.content)
            elseif e.name == "endDate"
                enddate= parse(Date, e.content)
            else 
                @error "Unknown period definition."
            end
        end
        push!(df_context, Dict([
            :context => c["id"],
            :startdate => startdate,
            :enddate => enddate,
            :instant => instant
        ]))
    end
    df_context
end


end # module

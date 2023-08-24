module Parser

using EzXML
using Dates
using DataFrames
using Parquet2: Dataset, writefile
using ProgressBars

import Logging
Logging.disable_logging(Logging.Warn)

include("./Paths.jl")
using .Paths

include("./Xbrl.jl")
using .Xbrl

include("./Taxonomies.jl")
using .Taxonomies

export gaap_data

function gaap_data(form::String, year::Int, quarter::Int)
    df_index = DataFrame(Dataset("index/index.parquet"))
    df_index = filter(Symbol("Form Type") => x -> x.==form, df_index)
    df_index = filter(:year => x -> x.==year, df_index)
    df_index = filter(:quarter => x -> x.==quarter, df_index)
end

function gaap_data(df_index::DataFrame)
    df_gaap = DataFrame()
    for row in ProgressBar(eachrow(df_index))
        df = gaap_data(row.CIK, row.filing_id)
    end
end

function gaap_data(cik::Int, filing_id::String)
    # Get the raw data
    df_gaap = gaap_data_raw(cik, filing_id)
    if nrow(df_gaap) > 1
        # Add all possible missing information using calculations
        augment!(df_gaap, cik, filing_id)
        # Add the context (dates)
        df_context = Xbrl.context_data(cik, filing_id)
        leftjoin!(df_gaap, df_context, on=:context)
        select!(df_gaap, Not(:context))
        date(instant, enddate) = ismissing(instant) ? enddate : instant 
        transform!(df_gaap, :, [:instant, :enddate] => ByRow(date) => :date)
    end
    df_gaap
end

function gaap_data_raw(cik::Int, filing_id::String)
    path = Paths.filing_path(cik, filing_id)
    instance = Xbrl.xbrl_instance(cik, filing_id)
    file_instance = path * instance
    doc = EzXML.readxml(file_instance)


    df_gaap = DataFrame()

    # Check that the document has a us-gaap namespace
    if "us-gaap" ∈ first.(namespaces(doc.root))
        # Get the us-gaap values
        gaap_nodes = findall("//us-gaap:*", doc.root)
        # Extract data
        df_gaap = DataFrame(
            tag = String[],
            unit = String[],
            decimals = Int[],
            value = Float32[],
            context = String[]
        )
        for g in gaap_nodes
            if haskey(g, "unitRef") & haskey(g, "decimals")
                push!(df_gaap, Dict([
                    :tag => g.name,
                    :unit => g["unitRef"],
                    :decimals => parse(Int, g["decimals"]),
                    :value => parse(Float32, g.content),
                    :context => g["contextRef"]
                ]))
            end
        end
    end
    df_gaap
end

function augment!(df_gaap::DataFrame, cik::Int, filing_id::String)
    # Augment data using taxonomy
    df_cals = calculations(cik, filing_id)

    n_from = 1
    n_to = 1

    while (n_from + n_to > 0)
        for g in eachrow(df_gaap)
            # Find all FROM calculations involving the tag
            # Calculations primary key is (role_id, from)
            cals = unique(df_cals[df_cals.from.==g.tag, [:role_id, :from]])
            for cal in eachrow(cals)
                n_from = augment!(df_gaap, df_cals, cal.from, cal.role_id, g.context)
            end
            # Find all TO calculations involving the tag
            cals = unique(df_cals[df_cals.to.==g.tag, [:role_id, :from]])
            for cal in eachrow(cals)
                n_to = augment!(df_gaap, df_cals, cal.from, cal.role_id, g.context)
            end
        end
    end
end

function augment!(df_gaap::DataFrame, df_cals::DataFrame, from::String, role_id::String, context::String)

    nrow_start = nrow(df_gaap)
    c = df_cals[(df_cals.from .== from) .& (df_cals.role_id .== role_id), :]
    tags_c = unique([c.from..., c.to...])
    tags = df_gaap[[x ∈ tags_c for x in df_gaap.tag], :]
    tags = tags[tags.context .== context, :]

    if nrow(tags) == length(tags_c) - 1

        # Check the decimals
        decimals = tags.decimals[1]
        if length(unique(tags.decimals)) > 1
            @error "Different decimals values not supported"
        end
        # Check the units
        unit = tags.unit[1]
        if length(unique(tags.unit)) > 1
            @error "Different unit values not supported"
        end
        # Check the calculation arcrole
        arcroles = unique(c.arcrole)
        if length(arcroles) > 1
            @error "Non-unique arcrole for calculation"
        else
            arcrole = arcroles[1]
        end

        # Identify the missing tag
        unknown_tag = [t for t in tags_c if t ∉ tags.tag][1]

        # Augment the missing tag
        if arcrole == "summation-item"
            if unknown_tag == from
                # Equation: unknown = w_known * known
                known = leftjoin(tags, c[!, [:to, :weight]], on=:tag => :to)
                unknown_value = sum(known.value .* known.weight)
            else
                # Equation FROM = w_known * known + w_unknown*unknown (where w=+/-)
                # unknown = (1/w)(FROM - known)
                from_value = tags[tags.tag.==from, :value][1]
                tags = tags[tags.tag .!= from, :]
                known = leftjoin(tags, c[!, [:to, :weight]], on=:tag => :to)
                known_value = sum(known.value .* known.weight)
                unknown_weight = c[c.to .== unknown_tag, :weight][1]
                unknown_value = (1 / unknown_weight) * (from_value - known_value)
            end

            # println("Augmentation performed for role ", role_id, ": From ", c.from[1], " Tag: ", tag_to_augment)
            push!(df_gaap, Dict([
                :tag => unknown_tag,
                :unit => unit,
                :decimals => decimals,
                :value => unknown_value,
                :context => context
            ]))

        else
            @error "Arcrole '" * arcrole * "' non-supported"
        end
    elseif nrow(tags) == length(tags_c)
        # Check the data
        # Equation FROM = w_known * known
        from_value = tags[tags.tag.==from, :value][1]
        tags = tags[tags.tag .!= from, :]
        known = leftjoin(tags, c[!, [:to, :weight]], on=:tag => :to)
        known_value = sum(known.value .* known.weight)
        if from_value != known_value
            mess = "Assertion error for role " * role_id * ": From " * c.from[1]
            @error mess
        end
        # @assert from_value == known_value mess
    end

    nrow_end = nrow(df_gaap)
    return(nrow_end - nrow_start)
end

end # module


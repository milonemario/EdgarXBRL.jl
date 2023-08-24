module Taxonomies

using HTTP
using Dates
using EzXML
using DataFrames
using ZipFile

import Logging
Logging.disable_logging(Logging.Warn)

include("./Paths.jl")
using .Paths

include("./Xbrl.jl")
using .Xbrl

export roles
export calculations

struct Taxonomy
    url::String
    name::String
end

function root_taxonomies()
    "taxonomies/"
end

function path_taxonomy(taxonomy::Taxonomy)
    root_taxonomies() * taxonomy.name * "/"
end

function taxonomy(cik::Int, filing_id::String)
    path = Paths.filing_path(cik, filing_id)
    schemas = Xbrl.xbrl_schemas(cik, filing_id)
    files_schemas = [path * s for s in schemas]
    # Get the gaap taxonomy used
    tax = Taxonomy("http://xbrl.fasb.org/us-gaap/2011/", "us-gaap-2011-01-31")
    for f in files_schemas
        doc = EzXML.readxml(f)
        imports = findall("//*[@schemaLocation]", doc.root)
        for i in imports
            if occursin("fasb.org/us-gaap", i["schemaLocation"])
                tax_name = split(basename(i["schemaLocation"]), ".")[1]
                tax_url = split(i["schemaLocation"], "elts")[1]
                tax = Taxonomy(tax_url, tax_name)
            end
        end
    end
    tax
end

function download_taxonomy(taxonomy::Taxonomy)
    println("Download taxonomy ", taxonomy.name)
    url_zip = taxonomy.url * taxonomy.name * ".zip"
    path = root_taxonomies()
    filename = taxonomy.name * ".zip"

    if !isdir(path)
        mkpath(path)
    end
    write(path * filename, HTTP.get(url_zip).body)
    zarchive = ZipFile.Reader(path * filename)
    for f in zarchive.files
        dir = dirname(f.name)
        if !isdir(path * dir)
            mkpath(path * dir)
        end
        write(path * f.name, read(f))
    end
    close(zarchive)
    rm(path * filename)
end

function roles(cik::Int, filing_id::String)
    tax = taxonomy(cik, filing_id)
    path = path_taxonomy(tax)
    if !isdir(path)
        download_taxonomy(tax)
    end
    file_roles = path * "elts/us-roles-" * taxonomy.name * ".xsd"
    doc_roles = EzXML.readxml(file_roles)
    roles_xml = findall("//link:roleType", doc_roles.root)

    df_roles = DataFrame(
        role_id = String[],
        role = String[],
        number = Int[],
        name = String[]
    )

    for r in roles_xml
        usages = [u.content for u in findall("link:usedOn", r)]
        if "link:calculationLink" ∈ usages
            definition = findfirst("link:definition", r).content
            push!(df_roles, Dict([
                :role_id => r["id"],
                :role => split(r["roleURI"], "/")[end],
                :number => parse(Int, split(definition, " - ")[1]),
                :name => definition 
            ]))
        end
    end

    df_roles
end

function calculations(cik::Int, filing_id::String)
    tax = taxonomy(cik, filing_id)
    path = path_taxonomy(tax)
    if !isdir(path)
        download_taxonomy(tax)
    end
    files_cal = []
    for (root, dirs, files) in walkdir(path)
        for f in files
            if occursin("-cal-", f)
                push!(files_cal, root * "/" * f)
            end
        end
    end
    
    df_cals = DataFrame(
        role_id = String[],
        arcrole = String[],
        from = String[],
        to = String[],
        weight = Float32[]
    )

    for f in files_cal
        doc = EzXML.readxml(f)
        role_id = split(findfirst("//link:roleRef", doc.root)["xlink:href"], "#")[end]
        # Get locators
        locs_xml = findall("//link:loc", doc.root)
        locs = Dict([l["xlink:label"] => split(l["xlink:href"], "#us-gaap_")[end] for l in locs_xml])
        # Get calculations
        cals_xml = findall("//link:calculationArc", doc.root)
        for c in cals_xml
            push!(df_cals, Dict([
                :role_id => role_id,
                :arcrole => split(c["xlink:arcrole"], "/")[end],
                :from => locs[c["xlink:from"]],
                :to => locs[c["xlink:to"]],
                :weight => parse(Float32, c["weight"])
            ]))
            
        end
    end

    df_cals
end

end # module

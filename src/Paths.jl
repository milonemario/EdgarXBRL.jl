module Paths

function filing_url(cik::Int, filing_id::String)
    sec_url = "https://www.sec.gov/Archives/edgar/data/"
    filing_id_condensed = replace(filing_id, "-" => "")
    sec_url * string(cik) * "/" * filing_id_condensed * "/"
end

function filing_path(cik::Int, filing_id::String)
    filing_id_condensed = replace(filing_id, "-" => "")
    "filings/" * string(cik) * "/" * filing_id_condensed * "/"
end

function filing_index_filename(filing_id::String)
    filing_id * "-index.html"
end

end # module

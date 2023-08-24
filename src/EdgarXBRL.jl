module EdgarXBRL

include("./Paths.jl")
using .Paths

include("./Downloader.jl")
using .Downloader

export download_data

include("./Parser.jl")
using .Parser

export gaap_data

include("./Taxonomies.jl")
using .Taxonomies

include("./Index.jl")
using .Index

export update_index

include("./Xbrl.jl")
using .Xbrl

end # module

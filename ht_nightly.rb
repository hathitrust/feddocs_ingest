# update preexisting HathiTrust records
# add new HathiTrust records existing Registry Records
# create new Registry Records if no match can be found
# we are NOT recalculating clustering!!!
#
require 'registry/registry_record'
require 'registry/source_record'
require 'normalize'
require 'json'
require 'dotenv'
require 'pp'
include Registry::Series

SourceRecord = Registry::SourceRecord
RegistryRecord = Registry::RegistryRecord
begin
Mongoid.load!(File.expand_path("../config/mongoid.yml", __FILE__), :development)

ORGCODE = 'miaahdl'
OCLCPAT = 
  /
      \A\s*
      (?:(?:\(OCo?LC\)) |
  (?:\(OCo?LC\))?(?:(?:ocm)|(?:ocn)|(?:on))
  )(\d+)
  /x

fin = ARGV.shift
puts fin
if fin =~ /\.gz$/
  updates = Zlib::GzipReader.open(fin)
else
  updates = open(fin)
end
new_count = 0
update_count = 0
src_count = {}
rr_ids = []
count = 0
rrcount = 0
updates.each do | line | 
  count += 1 

  line.chomp!

  new_src = SourceRecord.new
  new_src.org_code = "miaahdl"
  new_src.source = line
 
  # fuhgettaboutit 
  if !new_src.is_govdoc and (SourceRecord.where(org_code:ORGCODE,
                                                local_id:new_src.local_id,
                                                deprecated_timestamp:{"$exists":0}).count == 0)

    next
  end

  marc = JSON.parse line

  field_008 = marc['fields'].find {|f| f['008'] }['008']

     
  # pre-existing source record that has been updated
  src = SourceRecord.where(org_code: ORGCODE, local_id: new_src.local_id).first
  if src
    src_count[src.source_id] = 0

    #trust that it's an improvement
    src.source = line
    src.in_registry = true
    src.save
    res = src.update_in_registry "HT update: #{fin}"
    update_count += 1

    rrcount += res[:num_new]
    src_count[src.source_id] += 1
  #new source record
  else 
    new_src.in_registry = true
    new_src.save
    #puts "new source: #{htid}"
    new_count += 1

    src_count[new_src.source_id] = 0

    res = new_src.add_to_registry "HT update: #{fin}"
    rrcount += res[:num_new]
    src_count[new_src.source_id] += 1
  end

end
puts "regrec count: #{rrcount}"
puts "new srcs: #{new_count}"
puts "updates: #{update_count}"
#PP.pp src_count 

rescue Exception => e
  PP.pp e
  puts e.backtrace
end


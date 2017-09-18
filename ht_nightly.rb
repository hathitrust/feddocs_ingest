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
#tracking why it's a govdoc
author_count = 0
oclc_count = 0
gpo_num_count = 0
sudoc_count = 0
count_008 = 0

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

    # tracking why we included it
    if new_src.marc['008'].value !~ /^.{17}u.{10}f/
      count_008 += 1
      sudoc_count += 1 if new_src.sudocs.count > 0
      gpo_num_count += 1 if new_src.gpo_item_numbers.count > 0 
      author_count += 1 if new_src.has_approved_author?
      if new_src.sudocs.count == 0 and
        new_src.gpo_item_numbers.count == 0 and
        !new_src.has_approved_author?
        oclc_count += 1
      end
    end
  end

end
puts "regrec count: #{rrcount}"
puts "updates: #{update_count}"
#puts "would have been included based on author: #{author_count}"
puts "new srcs: #{new_count}"
puts "# of new sources without an 008 indicating GovDoc: #{count_008}"
puts "# of new sources with Author indicating GovDoc: #{author_count}"
puts "# of new sources with GPO# indicating GovDoc: #{gpo_num_count}"
puts "# of new sources with SuDoc indicating GovDoc: #{sudoc_count}"
puts "# of new sources with OCLC indicating GovDoc: #{oclc_count}"
#PP.pp src_count 

rescue Exception => e
  PP.pp e
  puts e.backtrace
end


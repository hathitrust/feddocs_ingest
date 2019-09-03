# Daily updates give use new/changed records. 
# We need to find records that have been deleted from Zephir and
# handle appropriately. 
require 'registry/registry_record'
require 'registry/source_record'
require 'normalize'
require 'json'
require 'dotenv'
require 'pp'

SourceRecord = Registry::SourceRecord
RegistryRecord = Registry::RegistryRecord
Mongoid.load!(File.expand_path(ENV['MONGOID_CONF'], __FILE__), :production)

ORGCODE = 'miaahdl'
OCLCPAT = 
  /
      \A\s*
      (?:(?:\(OCo?LC\)) |
  (?:\(OCo?LC\))?(?:(?:ocm)|(?:ocn)|(?:on))
  )(\d+)
  /x

puts DateTime.now
fin = ARGV.shift
puts fin
if fin =~ /\.gz$/
  zeph = Zlib::GzipReader.open(fin)
else
  zeph = open(fin)
end
count = 0
all_zeph_ids = Hash.new(0)
gd_zeph_ids = Hash.new(0)
non_gd_ids = Hash.new(0)
deleted_zeph_ids = Hash.new(0)
new_zeph_ids = Hash.new(0)
new_holdings_ids = Hash.new(0)
disappearing_holdings_ids = Hash.new(0)
new_ecs_count = 0
new_regrec_count = 0
deleted_ecs_count = 0
added_entry_count = 0
ae_out = open(__dir__+'/monthly_reports/added_entry_'+Date.today.strftime('%Y_%m_%d')+'.txt', 'w')
zeph.each do | line | 
  count += 1 

  line.chomp!

  src = SourceRecord.new
  src.org_code = "miaahdl"
  src.source = line
  all_zeph_ids[src.local_id] = 1
   
  # fuhgettaboutit 
  if !src.fed_doc? and (SourceRecord.where(org_code:ORGCODE,
                                            local_id:src.local_id,
                                            deprecated_timestamp:{"$exists":0}).count == 0)
    next
  elsif src.fed_doc?
    gd_zeph_ids[src.local_id] = 1
  end

  old_src = SourceRecord.where(org_code: ORGCODE, local_id: src.local_id).first
  if !old_src # a newbie, just add it
    src.save
    if !src.u_and_f? and
        !src.sudocs.any? and
        !src.gpo_item_numbers.any? and
        !src.approved_author? and
        src.approved_added_entry? 
      ae_out.puts [src.source_id, 
                   src.oclc_resolved.join(', '),
                   (src.author || []).join(', '),
                   (src.publisher || []).join(', ')
                  ].join("\t")
      added_entry_count += 1
    end
    new_zeph_ids[src.local_id] += 1
    res = src.add_to_registry "HT Monthly update: #{fin}"
    new_ecs_count += res[:num_new]
  else 
    # the old list of enum_chrons doesn't match the new list
    # if registry_src.enum_chrons & old_src.enum_chrons != src.enum_chrons
    old_src.source = line
    old_src.save
    if !old_src.u_and_f? and
        !old_src.sudocs.any? and
        !old_src.gpo_item_numbers.any? and
        !old_src.approved_author? and
        old_src.approved_added_entry? 
      ae_out.puts [old_src.source_id, 
                   old_src.oclc_resolved.join(', '),
                   (old_src.author || []).join(', '),
                   (old_src.publisher || []).join(', ')
                  ].join("\t")
      added_entry_count += 1
    end
    res = old_src.update_in_registry "HT Monthly update: #{fin}"
    new_ecs_count += res[:num_new]
    new_holdings_ids[old_src.local_id] += res[:num_new]
    deleted_ecs_count += res[:num_deleted]
    disappearing_holdings_ids[old_src.local_id] += res[:num_deleted]
  end
end

#only want source records created before file date
date = fin.match(/(?<year>\d{4})(?<month>\d{2})(?<day>\d{2})/)
#mongo's objectids have their creation date in them, making it the 
#easiest way to search by insertion date
objid = BSON::ObjectId.from_time(Time.new(date[:year],date[:month],date[:day]))

nongd = open(__dir__+'/monthly_reports/nongd_in_registry_'+Date.today.strftime('%Y_%m_%d')+'.tsv', 'w')
SourceRecord.where(org_code: ORGCODE, 
                   "_id":{"$lt":objid},
                   deprecated_timestamp:{"$exists":0}).no_timeout.each do |src|
  if !gd_zeph_ids.has_key?(src.local_id) and 
    !all_zeph_ids.has_key?(src.local_id)
    res = src.remove_from_registry "Not found in HT Monthly update: #{fin}"
    src.deprecate "Not found in HT Monthly update: #{fin}"
    deleted_ecs_count += res
    deleted_zeph_ids[src.local_id] += 1
  elsif !gd_zeph_ids.has_key?(src.local_id) and
    all_zeph_ids.has_key?(src.local_id)
    non_gd_ids[src.local_id] += 1
    nongd.puts src.enum_chrons.count.to_s+"\t"+src.local_id+"\t"+src.oclc_resolved.join(", ")
  #else we're good
  end
end 

summ_out = open(__dir__+'/monthly_reports/summary_'+Date.today.strftime('%Y_%m_%d')+'.txt', 'w')
summ_out.puts "# of ids in Zephir: #{all_zeph_ids.keys.count}"
summ_out.puts "# of GD ids in Zephir: #{gd_zeph_ids.keys.count}"
summ_out.puts "# of new Zephir ids: #{new_zeph_ids.keys.count}"
summ_out.puts "# of Zephir ids with disappearing holdings: #{disappearing_holdings_ids.keys.count}"
summ_out.puts "# of Zephir ids with new holdings: #{new_holdings_ids.keys.count}"
summ_out.puts "# of deleted Zephir ids: #{deleted_zeph_ids.keys.count}"
summ_out.puts "# of non-gd Zephir ids in registry: #{non_gd_ids.keys.count}"

{new_zeph_ids:new_zeph_ids, 
 disappearing_holdings_ids:disappearing_holdings_ids, 
 new_holdings_ids:new_holdings_ids, deleted_zeph_ids:deleted_zeph_ids, 
 non_gd_ids:non_gd_ids}.each do | k, ids |
  s_out = open(__dir__+"/monthly_reports/#{k}_"+Date.today.strftime('%Y_%m_%d')+'.txt', 'w')
  ids.each do |id, somecount|
    s_out.puts [id, somecount].join("\t")
  end
end
puts DateTime.now
puts "number new with approved added_entry:#{added_entry_count}"

# fixing ht_ids_fv/lv and ht_availability problems
# caused by bug in source_record class
# Should be good after 3/15/2016 
require 'registry_record'
require 'source_record'
require 'json'
require 'dotenv'
require 'pp'

Mongoid.load!("config/mongoid.yml", :development)

fout = open("unmatched.txt", 'w')

match_count = 0
nomatch_count = 0
line_number = 0
SourceRecord.where(:org_code => "miaahdl").no_timeout.each do |source|
  RegistryRecord.where(:source_record_ids => source.source_id).each do |reg|
    if source.ht_availability == 'Full View'
      reg.ht_ids_fv << source.local_id
      reg.ht_ids_fv.uniq!
      reg.save()
    else 
      reg.ht_ids_lv << source.local_id
      reg.ht_ids_lv.uniq!
      reg.save()
    end
    reg.set_ht_availability()
    reg.save()
  end
end 
    

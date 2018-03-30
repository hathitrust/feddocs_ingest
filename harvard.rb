# Ingest Harvard monographs from https://library.harvard.edu/open-metadata
require 'registry/registry_record'
require 'registry/source_record'
require 'normalize'
require 'json'
require 'dotenv'
require 'pp'
require 'marc'
include Registry::Series

SR = Registry::SourceRecord
RR = Registry::RegistryRecord
Mongoid.load!(File.expand_path(ENV['MONGOID_CONF'], __FILE__), :production)

encoding_options = { 
  :external_encoding => "UTF-8",
  :invalid => :replace,
  :undef   => :replace,
  :replace => '', 
}


puts DateTime.now

num_new = 0
num_existing = 0
# data/hlom/bunch of mrc files
ARGV.each do |mh_mrc|
  puts mh_mrc
  reader = MARC::Reader.new(open(mh_mrc), encoding_options)
  reader.each do |rec|
    src = SR.new(org_code:"mh", 
                           source: rec.to_hash.to_json)
    next unless src.monograph?
    
    # fuhgettaboutit 
    next if !src.fed_doc? and
      (SR.where(org_code:"mh",
                          local_id:src.local_id,
                          deprecated_timestamp:{"$exists":0}).count == 0)


  
    old_src = SR.where(org_code:"mh", local_id: src.local_id).first
    if !old_src # a newbie, just add it
      src.save
      res = src.add_to_registry "Harvard update: #{mh_mrc}"
      num_new += 1
    else #update source in old record
      old_src.source = line
      old_src.save
      res = old_src.update_in_registry "Harvard update: #{fin}"
      num_existing += 1
    end
  end #each rec
end

puts "num new:#{num_new}"
puts "num existing:#{num_existing}"
puts DateTime.now

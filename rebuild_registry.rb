# Rebuild the registry using cluster files
#
require 'registry_record'
require 'source_record'
require 'json'
require 'dotenv'
require 'pp'

Mongoid.load!("config/mongoid.yml", :development)

CUTOFF = 0.9 #dupe scores lower than .9 are ignored

clusters = open(ARGV.shift)
gme = open(ARGV.shift)
enumchrons = {}
gme.each do | line |
  l = line.chomp.split(/\t/)
  enumchrons[l[0]] ||= [l[1],[]]
  enumchrons[l[0]][1] << l[2]
end

count = 0
line_number = 0
clusters.each do | line | 
  line_number += 1
  c = line.chomp.split(/\t/)
  if c[1].to_f < CUTOFF or c[1].to_f >= 1.0
    next
  end

  gdids = c[2].split(/,/)
  shared_ecs = []
  all_ecs = gdids.collect {|gd| enumchrons[gd][1]}.flatten.uniq
  all_ecs.each do |ec| 
    ec_group = gdids.select {|gd| enumchrons[gd][1].include? ec} 
    if ec == 'NULL'
      ec = ''
    end
    cluster = ec_group.collect {|gd| enumchrons[gd][0]}
    #make sure we don't already have it
    #if RegistryRecord.where(:source_record_ids => cluster,
		#	    :enumchron_display => ec).first
    #  next
    #end
    regrec = RegistryRecord.new(cluster, ec, 'build Feb 2016. >= .9')
    regrec.save()
    count += 1
    if count % 10000 == 0
      print "\rcount #{count}"
    end

  end
end
puts count


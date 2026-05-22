require "sync/mutex"
require "./config"

module Raft
  class Metrics
    HISTOGRAM_BUCKETS = [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]

    @node_id : NodeID
    @group_id : UInt64
    @gauges : Hash(String, Int64) = Hash(String, Int64).new(0_i64)
    @counters : Hash(String, Int64) = Hash(String, Int64).new(0_i64)
    @histograms : Hash(String, HistogramData) = {} of String => HistogramData
    @mutex : Sync::Mutex = Sync::Mutex.new

    struct HistogramData
      property buckets : Array(Int64)
      property sum : Float64
      property count : Int64
      property labels : Hash(String, String)

      def initialize(@labels : Hash(String, String) = {} of String => String)
        @buckets = Array(Int64).new(HISTOGRAM_BUCKETS.size + 1, 0_i64)
        @sum = 0.0
        @count = 0_i64
      end
    end

    def initialize(@node_id : NodeID, @group_id : UInt64 = 0_u64)
    end

    def set_gauge(name : String, value : Int64)
      @mutex.synchronize { @gauges[name] = value }
    end

    def get_gauge(name : String) : Int64
      @mutex.synchronize { @gauges[name] }
    end

    def increment(name : String, by : Int64 = 1_i64)
      @mutex.synchronize { @counters.update(name) { |v| v + by } }
    end

    def increment(name : String, labels : Hash(String, String), by : Int64 = 1_i64)
      @mutex.synchronize do
        key = "#{name}{#{labels.map { |k, v| "#{k}=\"#{v}\"" }.join(",")}}"
        @counters.update(key) { |v| v + by }
      end
    end

    def get_counter(name : String) : Int64
      @mutex.synchronize { @counters[name] }
    end

    def observe(name : String, value : Float64, labels : Hash(String, String) = {} of String => String)
      @mutex.synchronize do
        key = labels.empty? ? name : "#{name}{#{labels.map { |k, v| "#{k}=\"#{v}\"" }.join(",")}}"
        hist = @histograms[key]? || HistogramData.new(labels)
        hist.sum += value
        hist.count += 1
        HISTOGRAM_BUCKETS.each_with_index do |bound, i|
          hist.buckets[i] += 1 if value <= bound
        end
        hist.buckets[HISTOGRAM_BUCKETS.size] += 1 # +Inf always
        @histograms[key] = hist
      end
    end

    def to_prometheus : String
      String.build { |io| to_prometheus(io) }
    end

    def to_prometheus(io : IO) : Nil
      @mutex.synchronize do
        base_labels = "node_id=\"#{@node_id}\",group_id=\"#{@group_id}\""
        @gauges.each do |name, value|
          io << name << "{" << base_labels << "} " << value << "\n"
        end
        @counters.each do |key, value|
          if key.includes?('{')
            name = key.split('{').first
            extra_labels = key.split('{').last.rstrip('}')
            io << name << "{" << base_labels << "," << extra_labels << "} " << value << "\n"
          else
            io << key << "{" << base_labels << "} " << value << "\n"
          end
        end
        @histograms.each do |key, hist|
          base_name = key.includes?('{') ? key.split('{').first : key
          label_str = String.build do |ls|
            ls << base_labels
            hist.labels.each do |k, v|
              ls << "," << k << "=\"" << v << "\""
            end
          end
          HISTOGRAM_BUCKETS.each_with_index do |bound, i|
            io << base_name << "_bucket{" << label_str << ",le=\"" << bound << "\"} " << hist.buckets[i] << "\n"
          end
          io << base_name << "_bucket{" << label_str << ",le=\"+Inf\"} " << hist.buckets[HISTOGRAM_BUCKETS.size] << "\n"
          io << base_name << "_sum{" << label_str << "} " << hist.sum << "\n"
          io << base_name << "_count{" << label_str << "} " << hist.count << "\n"
        end
      end
    end
  end
end

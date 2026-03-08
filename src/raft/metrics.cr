module Raft
  class Metrics
    HISTOGRAM_BUCKETS = [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]

    @node_id : NodeID
    @gauges : Hash(String, Int64) = {} of String => Int64
    @counters : Hash(String, Int64) = {} of String => Int64
    @histograms : Hash(String, HistogramData) = {} of String => HistogramData
    @mutex : Mutex = Mutex.new

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

    def initialize(@node_id : NodeID)
    end

    def set_gauge(name : String, value : Int64)
      @mutex.synchronize { @gauges[name] = value }
    end

    def get_gauge(name : String) : Int64
      @mutex.synchronize { @gauges.fetch(name, 0_i64) }
    end

    def increment(name : String, by : Int64 = 1_i64)
      @mutex.synchronize { @counters[name] = @counters.fetch(name, 0_i64) + by }
    end

    def get_counter(name : String) : Int64
      @mutex.synchronize { @counters.fetch(name, 0_i64) }
    end

    def observe(name : String, value : Float64, labels : Hash(String, String) = {} of String => String)
      @mutex.synchronize do
        key = labels.empty? ? name : "#{name}{#{labels.map { |k, v| "#{k}=#{v}" }.join(",")}}"
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
      @mutex.synchronize do
        String.build do |io|
          @gauges.each do |name, value|
            io << name << "{node_id=\"" << @node_id << "\"} " << value << "\n"
          end
          @counters.each do |name, value|
            io << name << "{node_id=\"" << @node_id << "\"} " << value << "\n"
          end
          @histograms.each do |key, hist|
            base_name = key.includes?('{') ? key.split('{').first : key
            label_str = String.build do |ls|
              ls << "node_id=\"" << @node_id << "\""
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
end

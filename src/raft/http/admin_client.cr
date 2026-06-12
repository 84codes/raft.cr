require "http/client"
require "json"
require "uri"
require "../config"

module Raft
  module HTTP
    # Client side of the `/raft/admin/*` wire format. Keeps request shapes in
    # one place so embedders don't hand-roll them.
    #
    # Single attempt per call, no retry loop — embedders own retry policy.
    # Returns the response `::HTTP::Status` so callers can distinguish
    # rejection (400, e.g. already a member — stop retrying) from
    # unavailability (5xx — retry later). Raises only on connection errors.
    module AdminClient
      # POST `<uri>/raft/admin/add_server/<node_id>` with a JSON body
      # `{"address": address}`. Basic auth is taken from the URI's userinfo
      # when present.
      def self.add_server(uri : URI, node_id : NodeID, address : String = "") : ::HTTP::Status
        client = ::HTTP::Client.new(uri)
        begin
          if user = uri.user
            client.basic_auth(user, uri.password || "")
          end
          path = uri.path.rstrip('/') + "/raft/admin/add_server/#{node_id}"
          headers = ::HTTP::Headers{"Content-Type" => "application/json"}
          body = {address: address}.to_json
          response = client.post(path, headers: headers, body: body)
          response.status
        ensure
          client.close
        end
      end
    end
  end
end

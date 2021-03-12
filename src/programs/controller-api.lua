clusterd = {}
api = internal

function join(delim, as)
   v = ""
   for i, a in ipairs(as) do
      if i ~= 1 then
         v = v .. delim
      end
      v = v .. a
   end
   return v
end

function output(x)
   if type(x) ~= "string" then
      x = tostring(x)
   end

   coroutine.yield(x)
end

clusterd.output = output

------------------------------------------
-- System Commands                      --
------------------------------------------

function clusterd.system_status()
   res, err = api.run([[SELECT COUNT(n_id) AS count FROM node]])
   if err ~= nil or #res == 0 then
      error("Could not get node count")
   else
      output("Total Nodes: " .. res[1].count)
   end
end

------------------------------------------
-- Failure Domains                      --
------------------------------------------

function clusterd.set_failure_domains(domains)
   assert(type(domains) == "table", "domains must be a list of domain, description tables")

   parent = nil
   for _, d in ipairs(domains) do
      assert(type(d.name) == "string", "failure domain name must be a string")
      assert(d.description == nil or type(d.description) == "string",
             "failure domain description must be a string, if given")

      -- TODO, should check if there is another failure domain with
      -- this as the parent. If so, and the name is different, delete
      -- it.

      _, err = api.run(
         [[REPLACE INTO failure_domain(fd_name, fd_description, fd_parent)
                 VALUES ($name, $description, $parent)]],
         { name = d.name, description = d.description, parent = parent }
      )

      if err ~= nil then
         error("Could not update failure domain " .. d.name .. ": " .. err)
      else
         output(d.name)
      end

      parent = d.name
   end
end

function clusterd.list_failure_domains()
   rs, err = api.run([[SELECT fd_name, fd_description, fd_parent FROM failure_domain]])
   if err ~= nil then
      error("Could not get failure domains: " .. err)
   else
      for _, r in ipairs(rs) do
         output(join("\t", {r.fd_name, r.fd_description or '', r.fd_parent or ''}))
      end
   end
end

------------------------------------------
-- Node Resource Classes                --
------------------------------------------

function rc_to_json(rc)
   return json.encode({name = rc.rc_name,
                       fungible = rc.rc_fungible,
                       quantifiable = rc.rc_quantifiable,
                       description = rc.rc_description})
end

function clusterd.update_resource_class(nm, options)
   assert(type(nm) == "string", "resource class name must be a string")

   if options == nil then
      options = {}
   end

   assert(options.description == nil or type(options.description) == "string",
          "resource class description must be a string, if given")
   assert(options.fungible == nil or type(options.fungible) == "boolean",
          "resource class fungible must be a boolean, if given")
   assert(options.quantifiable == nil or type(options.quantifiable) == "boolean",
          "resource class quantifiable must be a boolean, if given")
   assert(options.parent == nil or type(options.parent) == "string",
          "resource class parent must be a string, if given")

   _, err = api.run(
      [[REPLACE INTO resource_class(rc_name, rc_fungible, rc_quantifiable, rc_description, rc_parent)
        VALUES ($name, $fungible, $quantifiable, $description, $parent)]],
      { name = nm,
        fungible = options.fungible or false,
        quantifiable = options.quantifiable or false,
        description = options.description,
        parent = options.parent }
   )
   if err ~= nil then
      error("Could not perform resource class replace: " .. err)
   else
      output(nm)
   end
end

function clusterd.list_resource_class()
   ds, err = api.run(
      [[SELECT resource_class(rc_name, rc_fungible, rc_description
        FROM resource_class]]
   )
   if err ~= nil then
      error("Could not get resource class: " .. err)
   else
      for _, rc in ipairs(ds) do
         output(rc_to_json(rc))
      end
   end
end

function clusterd.get_resource_class(nm)
   assert(type(nm) == "string", "resource class name must be a string")

   ds, err = api.run(
      [[SELECT resource_class(rc_name, rc_fungible, rc_description)
        FROM resource_class WHERE rc_name=$name]],
      { name = nm }
   )
   if err ~= nil then
      error("Could not get resource class: " .. err)
   else
      if #ds == 1 then
         rc = ds[1]
         output(rc_to_json(rc))
      end
   end
end

------------------------------------------
-- Nodes                                --
------------------------------------------

function node_to_json(node)
   return json.encode({id = node.n_id,
                       hostname = node.n_hostname,
                       ip = node.n_ip})
end

function clusterd.update_node(node, options)
   assert(type(node) == "string", "node id must be a string")
   if options.hostname ~= nil then
      assert(type(options.hostname) == "string", "node hostname must be string or nil")
   end
   assert(type(options.ip) == "string" and api.is_valid_ip(options.ip), "IP is not a valid IP address")

   api.log(api.debug, "Would update " .. node .. "(hostname=" .. options.hostname or "none" ..
              ", ip=" .. options.ip .. ")")

   _, err = api.run(
      [[REPLACE INTO node(n_id, n_hostname, n_ip, n_state)
        VALUES ($id, $hostname, $ip, 'up')]],
      {id = node, hostname = options.hostname, ip = options.ip })
   if err ~= nil then
      error("Could not perform node replace: " .. err)
   end

   -- Now update failure domains
   if options.failure_domains ~= nil then
      assert(type(options.failure_domains) == "table", "options.failure_domains must be a table")

      for domain, value in pairs(options.failure_domains) do
         assert(type(value) == "string", "failure domain values must be strings")

         if #value == 0 then
            _, err = api.run(
               [[DELETE FROM node_failure_domain WHERE nfd_node=$node AND nfd_name=$domain]],
               { node =  node, domain = domain }
            )
         else
            _, err = api.run(
               [[REPLACE INTO node_failure_domain(nfd_node, nfd_name, nfd_value)
                 VALUES ($node, $name, $value)]],
               { node = node, name = domain, value = value })
         end
         if err ~= nil then
            error("Could not update failure domain " .. domain .. ": " .. err)
         end
      end
   end

   if options.resources ~= nil then
      api.log(api.debug, "Updating node resources")
      assert(type(options.resources) == "table", "options.resources must be a table")

      for resource, value in pairs(options.resources) do
         assert(type(value) == "number", "resource values must be integers")
         if value == 0 then
            _, err = api.run(
               [[DELETE FROM node_resource WHERE nrc_name=$name AND nrc_node=$node]],
               {name = resource, node = node}
            )
         else
            _, err = api.run(
               [[REPLACE INTO node_resource(nrc_name, nrc_node, nrc_amount)
                 VALUES ($name, $node, $amount)]],
               {name = resource, node = node, amount = value }
            )
         end

         if err ~= nil then
            error("Could not update resource " .. resource .. ": " .. err)
         end
      end
   end

   output(node)
end

function clusterd.delete_node(node_id)
   assert(type(node_id) == "string", "node_id must be a string")

   _, err = api.run([[DELETE FROM node WHERE n_id=$node]], {node=node_id})
   if err ~= nil then
      error("Could not delete node " .. node_id)
   else
      output(node_id)
   end
end

function clusterd.list_nodes()
   res, err = api.run([[SELECT n_id, n_hostname, n_ip FROM node]])
   if err ~= nil then
      error("Could not get nodes: " .. err)
   else
      for _, row in ipairs(res) do
         output(node_to_json(row))
      end
   end
end

function clusterd.get_node(nid)
   res, err = api.run(
      [[SELECT n_id, n_hostname, n_ip FROM node WHERE n_id=$id]],
      { id = nid }
   )
   if err ~= nil then
      error('could not get node: ' .. err)
   end

   if #res == 0 then
      error('node ' .. nid .. ' not found')
   end

   return res[1]
end

function clusterd.resolve_node(nid)
   res, err = api.run(
      [[SELECT n_id FROM node WHERE n_id=$id]],
      { id = nid }
   )
   if err ~= nil then
      error('error while looking for node: ' .. err)
   end

   if #res ~= 0 then
      return res[1].n_id
   end

   res, err = api.run(
      [[SELECT n_id FROM node WHERE n_hostname=$hostname]],
      { hostname = nid }
   )
   if err ~= nil then
      error('error while looking up node by hostname: ' .. err)
   end

   if #res ~= 0 then
      return res[1].n_id
   end

   res, err = api.run(
      [[SELECT n_id FROM node where n_ip=$ip]],
      { ip = nid }
   )
   if err ~= nil then
      error('error while looking up node by IP: ' .. err)
   end

   if #res ~= 0 then
      return res[1].n_id
   end

   return nil
end

------------------------------------------
-- Namespaces                           --
------------------------------------------

function clusterd.is_system_namespace(ns_id)
   return (ns_id == 0 or ns_id == 1)
end

function clusterd.add_namespace(name)
   assert(name == nil or type(name) == "string",
          "namespace label must be string, if given")

   res, err = api.run([[SELECT COALESCE(MAX(ns_id), 0) AS id FROM namespace]])
   if err ~= nil or #res ~= 1 then
      error("Could not add namespace: " .. err)
   end

   ns_id = res[1].id + 1

   _, err = api.run(
      [[INSERT INTO namespace(ns_id, ns_label)
        VALUES ($id, $label)]],
      {id = ns_id, label = name}
   )
   if err ~= nil then
      error("Could not add namespace: " .. err)
   else
      return ns_id
   end
end

function clusterd.get_namespace_by_label(nameorid)
   assert(type(nameorid) == "string", "namespace label must be a string")
   res, err = api.run(
      [[SELECT ns_id FROM namespace WHERE ns_label=$name]],
      { name = nameorid }
   )
   if err ~= nil then
      error("could not get namespace by label: " .. err)
   end

   if #res == 0 then
      return nil
   end

   if #res > 1 then
      error("more than one namespace with label " .. nameorid)
   end

   return res[1].ns_id
end

function clusterd.resolve_namespace(nameorid)
   ns_id = tonumber(nameorid)
   if ns_id == nil then
      -- find by name
      ns_id = clusterd.get_namespace_by_label(nameorid)
   end

   return ns_id
end

function clusterd.delete_namespace(nameorid)
   ns_id = clusterd.resolve_namespace(nameorid)

   if ns_id == nil then
      error("Namespace not found: " .. nameorid)
   end

   if clusterd.is_system_namespace(ns_id) then
      error("Cannot delete system namespace")
   end

   _, err = api.run(
      [[DELETE FROM namespace WHERE ns_id=$id]],
      { id = ns_id }
   )
   if err ~= nil then
      error("Could not delete namespace " .. nameorid .. ": " .. err)
   end

   output(ns_id)
end

function clusterd.get_namespace(nameorid)
   ns_id = clusterd.resolve_namespace(nameorid)

   if ns_id == nil then
      error("Namespace not found: " .. nameorid)
   end

   res, err = api.run(
      [[SELECT ns_id, ns_label FROM namespace WHERE ns_id=$id]],
      { id = ns_id }
   )
   if err ~= nil then
      error("Could not get namespace " .. nameorid .. ": " .. err)
   end

   if #res == 0 then
      error("Namespace " .. nameorid .. " does not exist")
   end

   return res[1]
end

function clusterd.list_namespaces()
   res, err = api.run([[SELECT ns_id, ns_label FROM namespace]])
   if err ~= nil then
      error("Could not get namespaces: " .. err)
   end

   return res
end

------------------------------------------
-- Services                             --
------------------------------------------

function clusterd.get_service_by_label(ns_id, svc)
   assert(type(ns_id) == "number", "namespace id must be a number")
   assert(type(svc) == "string", "service label must be a string")

   res, err = api.run(
      [[SELECT s_id FROM service WHERE s_namespace=$ns AND s_label=$label]],
      { ns = ns_id, label = svc }
   )
   if err ~= nil then
      error("could not get service by label: " .. err)
   end

   if #res == 0 then
      return nil
   end

   if #res > 1 then
      error("more than one service with label " .. svc .. " in namespace " .. ns_id)
   end

   return res[1].s_id
end

function clusterd.resolve_service(ns_id, svc)
   s_id = tonumber(svc)
   if s_id == nil then
      s_id = clusterd.get_service_by_label(ns_id, svc)
   end
   return s_id
end

function clusterd.get_service(ns, svc)
   ns_id = clusterd.resolve_namespace(ns)
   if ns_id == nil then
      error("namespace " .. ns .. " not found")
   end

   s_id = clusterd.resolve_service(ns_id, svc)
   if s_id == nil then
      error("service " .. svc .. " in namespace " .. ns_id .. " not found")
   end

   res, err = api.run(
      [[SELECT s_id, s_label, s_path FROM service
        WHERE s_id=$id]],
      { id = s_id }
   )
   if err ~= nil then
      error("could not get service " .. svc .. " in namespace " .. ns_id ..
               ": " .. err)
   end

   if #res < 1 then
      error("service " .. svc .. " in namespace " .. ns_id .. " not found")
   end

   return res[1]
end

function clusterd.update_service(ns, options)
   if options == nil then
      options = {}
   end
   assert(type(options) == "table", "service options must be a table")

   assert(options.label == nil or type(options.label) == "string",
          "service label must be a string, if given")
   assert(options.id == nil or type(options.id) == "number" or
             type(options.id) == "string",
          "service id must be a number or string, if given")

   ns_id = clusterd.resolve_namespace(ns)
   if ns_id == nil then
      error("namespace " .. ns .. " not found")
   end

   if options.id ~= nil then
      svc = clusterd.resolve_service(ns_id, options.id)
   end

   if svc ~= nil then
      -- This is a service update
      if options.create_only then
         error("service " .. options.id .. " already exists")
      end

      res, err =
         api.run(
            [[ SELECT s_id FROM service WHERE s_id=$id LIMIT 1 ]],
            { id = s_id }
         )
      if err ~= nil then
         error("could not lookup service " .. options.id .. " in namespace " .. ns ..
                  ": " .. err)
      end

      if #res == 0 then
         error("service " .. options.id .. " in namespace " .. ns .. ": not found")
      end
      action = "update"
      query =
         [[UPDATE service
           SET    s_label = COALESCE($label, s_label),
                  s_path  = COALESCE($path, s_path)
           WHERE  s_id    = $id]]
   else
      -- This is a service creation
      assert(type(options.path) == "string",
             "service path required on creation")

      if options.update_only then
         if options.id ~= nil then
            error("service " .. options.id .. " not found")
         else
            error("service id must be provided for update")
         end
      end

      res, err = api.run([[ SELECT MAX(s_id) AS s_id FROM service ]])
      if err ~= nil then
         error("could not generate service id: " .. err)
      end
      if #res == 0 then
         s_id = 1
      else
         s_id = (res[1].s_id or 0) + 1
      end

      action = "create"
      query =
         [[INSERT INTO service(s_id, s_namespace, s_label, s_path)
           VALUES ($id, $ns, $label, $path)]]
   end

   _, err = api.run(query, { id = s_id,
                             ns = ns_id,
                             label = options.label,
                             path = options.path })
   if err ~= nil then
      error("Could not " .. action .. " service " .. (options.id or '') ..
               ": " .. err)
   end

   return s_id
end

function clusterd.list_services(ns)
   ns_id = clusterd.resolve_namespace(ns)
   if ns_id == nil then
      error("namespace " .. ns .. " not found")
   end

   res, err = api.run(
      [[SELECT s_id, s_label, s_path FROM service WHERE s_namespace=$ns]],
      { ns = ns_id }
   )
   if err ~= nil then
      error("Could not get services: " .. err)
   end

   return res
end

function clusterd.delete_service(ns, svc)
   ns_id = clusterd.resolve_namespace(ns)
   if ns_id == nil then
      error("namespace " .. ns .. " not found")
   end

   s_id = clusterd.resolve_service(ns_id, svc)
   if s_id == nil then
      error("service " .. svc .. " not found in namespace " .. ns)
   end
   _, err = api.run(
      [[DELETE FROM service WHERE s_namespace=$ns AND s_id=$id]],
      { ns = ns_id, id = s_id }
   )
   if err ~= nil then
      error("could not delete service " .. svc .. " in namespace " .. ns ..
               ": " .. err)
   end
   output(svc)
end

------------------------------------------
-- Processes                            --
------------------------------------------

function clusterd.new_process(ns, svc, options)
   assert(ns ~= nil, "namespace required to create a process")
   assert(svc ~= nil, "service required to create a process")

   if options == nil then
      options = {}
   end

   ns_id = clusterd.resolve_namespace(ns)
   if ns_id == nil then
      error('namespace ' .. ns .. ' does not exist')
   end

   s_id = clusterd.resolve_service(ns_id, svc)
   if s_id == nil then
      error('service ' .. svc .. ' does not exist')
   end

   state = 'scheduling'

   -- Create a new process with the given options
   if options.placement ~= nil then
      state = 'starting'
   end

   -- Generate a new PID
   res, err = api.run(
      [[SELECT COALESCE(MAX(ps_id), 0) AS ps_id FROM process WHERE ps_ns=$namespace]],
      { namespace = ns_id }
   )
   if err ~= nil then
      error('could not get process id: ' .. err)
   end

   if #res == 0 then
      error('process id generation failed')
   end

   new_pid = res[1].ps_id + 1
   _, err = api.run(
      [[INSERT INTO process(ps_id, ps_svc, ps_ns, ps_state, ps_placement)
        VALUES ($id, $svc, $ns, $state, $placement)]],
      { id = new_pid, svc = s_id, ns = ns_id,
        state = state, placement = options.placement }
   )
   if err ~= nil then
      error('could not create new process entry ' .. err)
   end

   return new_pid
end

function clusterd.list_processes(ns, options)
   if options == nil then
      options = { resolve_names = false }
   end

   nsid = clusterd.resolve_namespace(ns)
   if nsid == nil then
      error('namespace ' .. ns .. ' not found')
   end

   if options.resolve_names then
      res, err = api.run(
         [[SELECT ps_id, ps_svc, ps_ns, ps_state, ps_placement,
                  s.s_label as ps_svc_name, ns.ns_label as ps_ns_name
           FROM process
           JOIN service s ON s.s_id = process.ps_svc
           JOIN namespace ns ON ns.ns_id = process.ps_ns
           WHERE process.ps_ns = $ns]],
         {ns = ns}
      )
   else
      res, err = api.run(
         [[SELECT ps_id, ps_svc, ps_ns, ps_state, ps_placement
           FROM process
           WHERE process.ps_ns = $ns]],
         {ns = ns}
      )
   end

   if err ~= nil then
      error("Could not get process list " .. err)
   end
   return res
end

function clusterd.update_process(ns, svc, options)
   assert(ns ~= nil, "namespace required to create or update process")
   assert(svc ~= nil, "service required to create or update process")

   if options == nil then
      options = {}
   end

   -- If an id is provided, it must exist
   if options.id ~= nil then
      lookup_process(ns, svc, options.id)
      pid = options.id
   end

   -- We currently do not check that a placement is correct. We need
   -- to set up some authorization functionality to handle this
   -- correctly. For now, we just trust the data.
end

return clusterd

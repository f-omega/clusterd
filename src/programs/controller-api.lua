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

function hexval(c)
   d = string.byte(c)
   if d >= 0x30 and d<0x3A then
      return d - 0x30
   elseif d >= 0x41 and d <= 0x46 then
      return (d - 0x41) + 10
   end
end

function clusterd.dehex(s)
   out = ""
   for i=0,(#s/2)-1 do
      local h = string.sub(s, i * 2 + 1, i * 2 + 1)
      local l = string.sub(s, i * 2 + 2, i * 2 + 2)

      out = out .. string.char(hexval(h) * 16 + hexval(l))
   end
   return out
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
   res, err = api.run([[SELECT ROWID as number, n_id AS id, n_hostname AS hostname, n_ip AS ip FROM node]])
   if err ~= nil then
      error("Could not get nodes: " .. err)
   end

   return res
end

function clusterd.get_node(nid)
   res, err = api.run(
      [[SELECT ROWID as number, n_id AS id, n_hostname AS hostname, n_ip AS ip FROM node WHERE n_id=$id]],
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

function clusterd.resolve_process(ns, pid)
   nsid = clusterd.resolve_namespace(ns)
   if nsid == nil then
      return nil
   end

   res, err = api.run(
      [[SELECT ps_id FROM process
        WHERE ps_ns = $ns AND ps_id = $pid]],
      { ns = ns, pid = pid }
   )
   if err ~= nil or #res ~= 1 then
      return nil
   end

   return res[1].ps_id
end

function clusterd.get_process(ns, pid)
   assert(ns ~= nil, "namespace required to create or update process")
   assert(pid ~= nil, "process ID required to create or update process")

   nsid = clusterd.resolve_namespace(ns)
   if nsid == nil then
      return nil
   end

   psid = clusterd.resolve_process(nsid, pid)
   if psid == nil then
      return nil
   end

   res, err = api.run(
      [[SELECT ps_id, ps_svc, ps_ns, ps_state, ps_placement
        FROM process WHERE ps_id=$pid AND ps_ns=$ns]],
      { pid = psid, ns = nsid }
   )
   if err ~= nil or #res ~= 1 then
      return nil
   end

   return res[1]
end

function clusterd.delete_process(ns, pid)
   assert(ns ~= nil, "namespace required to delete namespace")
   assert(pid ~= nil, "process ID required to delete namespace")

   nsid = clusterd.resolve_namespace(ns)
   if nsid == nil then
      return nil
   end

   psid = clusterd.resolve_process(nsid, pid)
   if psid == nil then
      return nil
   end

   _, err = api.run(
      [[DELETE FROM process WHERE ps_id=$pid AND ps_ns=$ns]],
      {pid = psid, ns = nsid}
   )
   if err ~= nil then
      error('could not delete process ' .. pid .. ' in namespace ' .. ns .. ': ' .. err)
   end
end

function clusterd.update_process(ns, pid, options)
   if options == nil then
      options = {}
   end

   process = clusterd.get_process(ns, pid)
   if process == nil then
      error('process ' .. pid .. ' not found in namespace ' .. ns)
   end

   if options.state ~= nil then
      if process.ps_state == 'zombie' and options.state ~= 'zombie' then
         error('cannot resurrect a zombie process')
      end

      _, err = api.run(
         [[UPDATE process
           SET    ps_state=$state
           WHERE  ps_id=$pid AND ps_ns=$ns]],
         { state = options.state, pid = process.ps_id, ns = process.ps_ns }
      )
      if err ~= nil then
         error('could not update process state: ' .. err)
      end
   end

   -- TODO need to use authentication or something to set placement
end

------------------------------------------
-- Global resources                     --
------------------------------------------

global_resource_projection =
   [[gr_ns AS ns, gr_name AS name, gr_management_process AS management_process,
     gr_metadata AS metadata, gr_type AS "type", gr_description AS description,
     gr_persistent AS persistent, gr_available AS available ]]

function clusterd.list_global_resources(options)
   conditions = {}

   if options.namespace ~= nil then
      ns = clusterd.resolve_namespace(options.namespace)
      if ns == nil then
         error('namespace ' .. options.namespace .. ' not found')
      end

      options.namespace = ns

      table.insert(conditions, "gr_ns=$namespace")
   end

   if #conditions > 0 then
      condition = [[ WHERE ]] .. table.concat(conditions, " AND ")
   else
      condition = ""
   end

   res, err = api.run([[SELECT ]] .. global_resource_projection ..
                      [[ FROM global_resource]] .. condition, options)
   if err ~= nil then
      error('could not list resources: ' .. err)
   end

   return res
end

function clusterd.get_global_resource(ns, name)
   assert(ns ~= nil, "namespace required to get global resource")
   assert(name ~= nil, "name required to get global resource")

   nsid = clusterd.resolve_namespace(ns)
   if nsid == nil then
      error('namespace ' .. ns .. ' not found')
   end

   rows, err = api.run(
      [[SELECT ]] .. global_resource_projection ..
      [[FROM global_resource
        WHERE gr_ns = $ns AND gr_name = $name]],
      {ns = nsid, name = name}
   )
   if err ~= nil then
      error('could not select from global_resource: ' .. err)
   end

   if #rows ~= 1 then
      return nil
   end

   res = rows[1]
   status, meta = pcall(function() return json.decode(res.metadata) end)
   if status then
      res.metadata = meta
   else
      res.bad_metadata = res.metadata
      res.metadata = {}
   end

   return res
end

function clusterd.new_global_resource(nsid, name, options)
   assert(nsid ~= nil, "namespace required to create global resources")
   assert(name ~= nil, "name required to create global resources")

   if options == nil then
      options = {}
   end

   assert(options.process ~= nil, "management process required to create a global resource")
   assert(options.type ~= nil, "resource type required to create a global resource")

   ns = clusterd.resolve_namespace(nsid)
   if ns == nil then
      error("namespace " .. nsid .. " not found")
   end

   mgmt_proc = clusterd.resolve_process(ns, options.process)
   if mgmt_proc == nil then
      error("process " .. options.process .. " in namespace " .. nsid .. " not found")
   end

   row = {
      name = name,
      ns = ns,
      management_process = mgmt_proc,
      type = options.type,
      description = options.description
   }

   if options.persistent ~= nil then
      row.persistent = options.persistent
   else
      row.persistent = false
   end

   if options.available ~= nil then
      row.available = options.available
   else
      row.available = false
   end

   if options.metadata == nil then
      options.metadata = {}
   end
   row.metadata = json.encode(options.metadata)

   _, err = api.run(
      [[INSERT INTO global_resource(gr_ns, gr_name, gr_management_process, gr_metadata,
                                    gr_type, gr_description, gr_persistent, gr_available)
        VALUES ($ns, $name, $management_process, $metadata, $type,
                $description, $persistent, $available)]],
      row
   )
   if err ~= nil then
      error('could not create global resource: ' .. err)
   end

   return name
end

function clusterd.delete_global_resource(ns, name)
   assert(ns ~= nil, "namespace required to delete global resource")
   assert(name ~= nil, "name required to delete global resource")

   resource = clusterd.get_global_resource(ns, name)
   if resource == nil then
      return
   end

   _, err = api.run(
      [[DELETE FROM global_resource WHERE gr_ns=$ns AND gr_name=$name]],
      {ns = resource.ns, name = resource.name}
   )
   if err ~= nil then
      error('could not delete global resource: ' .. err)
   end
end

function clusterd.update_global_resource(ns, name, options)
   assert(ns ~= nil, "namespace required to update global resource")
   assert(name ~= nil, "name required to update global resource")

   if options == nil then
      options = {}
   end

   assert(options.service == nil or options.force, "cannot update a global resource service once set, unless force given")
   assert(options.type == nil, "cannot update a global resource type")
   assert(options.persistent == nil, "cannot update resource persistence after creation")

   resource = clusterd.get_global_resource(ns, name)
   if resource == nil then
      error("global resource " .. name .. " in namespace " .. ns .. " not found")
   end

   updates = {}

   if options.description ~= nil and options.description ~= resource.description then
      updates.gr_description = options.description
   end

   if options.process ~= nil then
      service = clusterd.resolve_process(ns, options.process)
      if service == nil then
         error('process ' .. options.process .. ' in namespace ' .. ns .. ' not found')
      end

      updates.gr_management_process = process
   end

   if options.available ~= nil then
      updates.gr_available = options.available
      if not options.available and resource.available then
         -- If a resource is going to be made unavailabl, then it should have no assignments
         res, err = api.run(
            [[SELECT COUNT(*) AS count FROM global_resource_assignment
              WHERE gra_ns=$ns AND gra_resource=$resource]],
            { ns = resource.ns, resource = resource.name }
         )
         if err ~= nil or #res ~= 1 then
            error('cannot check resource assignments')
         end

         if res[1].count > 0 then
            error("cannot make resource unavailable, because it's assigned to " .. res[1].count .. " node(s)")
         end
      end
   end

   if options.metadata ~= nil then
      updates.gr_metadata = json.encode(options.metadata)
   end

   cols = {}
   for col, val in pairs(updates) do
      table.insert(cols, col .. '=$' .. col)
   end

   if #cols > 0 then
      query = [[UPDATE global_resource SET ]] .. table.concat(cols, ",") ..
         [[ WHERE gr_ns=$ns AND gr_name=$name ]]
      clusterd.output(query)

      updates.ns = resource.ns
      updates.name = resource.name

      _, err = api.run(query, updates)
      if err ~= nil then
         error('could not update global resource: ' .. err)
      end
   end
end

------------------------------------------
-- Global resource assignments          --
------------------------------------------

function clusterd.get_global_resource_assignments(ns, name)
   assert(ns ~= nil, 'namespace required to get resource assignments')
   assert(name ~= nil, 'name required to get resource assignments')

   nsid = clusterd.resolve_namespace(ns)
   if nsid == nil then
      error('namespace ' .. ns .. ' not found')
   end

   rows, err = api.run(
      [[SELECT gra_ns AS ns, gra_resource AS resource, gra_node AS node,
               gra_rel AS rel, gra_description AS description,
               gra_metadata AS metadata,
               gra_enforce_affinity AS enforce_affinity
        FROM global_resource_assignment
        WHERE gra_ns = $ns AND gra_resource = $name]],
      { ns = nsid, name = name }
   )
   if err ~= nil then
      error('could not get global resource assignments for ' .. name .. ' in namespace ' .. ns ..
               ': ' .. err)
   end

   for _, row in ipairs(rows) do
      status, metadata = pcall(function() return json.decode(row.metadata) end)
      if status then
         row.metadata = metadata
      else
         row.bad_metadata = row.metadata
         row.metadata = nil
      end
   end

   return rows
end

function clusterd.assign_global_resource(ns, name, node, opts)
   assert(ns ~= nil, 'namespace required to assign a global resource')
   assert(name ~= nil, 'resource name required to assign a global resource')
   assert(node ~= nil, 'node required to assign a global resource')

   if opts == nil then
      opts = {}
   end

   nodeid = clusterd.resolve_node(node)
   if nodeid == nil then
      error('node ' .. node .. ' not found')
   end

   nsid = clusterd.resolve_namespace(ns)
   if nsid == nil then
      error('namespace ' .. ns .. ' not found')
   end

   if opts.unassign then
      _, err = api.run(
         [[DELETE FROM global_resource_assignment
           WHERE gra_ns=$ns AND gra_resource=$name AND gra_node=$node AND gra_rel=COALESCE($rel, gra_rel)]],
         {ns = nsid, name = name, node = nodeid, rel = opts.rel}
      )
      if err ~= nil then
         error('could not unassign resource ' .. name .. ' in namespace ' .. ns .. ': ' .. err)
      end
   else
      resource = clusterd.get_global_resource(ns, name)
      if resource == nil then
         error('resource ' .. name .. ' in namespace ' .. ns .. ' not found')
      end

      if not resource.available then
         error('resource ' .. name .. ' in namespace ' .. ns .. ' is not available')
      end

      metadata = opts.metadata
      if metadata == nil then
         metadata = {}
      end

      _, err = api.run(
         [[INSERT INTO global_resource_assignment
            (gra_ns, gra_resource, gra_node, gra_rel,
             gra_description, gra_metadata, gra_enforce_affinity)
           VALUES ($ns, $resource, $node, COALESCE($rel, 'default'),
                   $description, $metadata, COALESCE($enforce_affinity, FALSE))
           ON CONFLICT (gra_ns, gra_resource, gra_rel)
           DO UPDATE SET gra_rel=COALESCE($rel, gra_rel),
                         gra_description=COALESCE($description, gra_description),
                         gra_metadata=$metadata,
                         gra_enforce_affinity=COALESCE($enforce_affinity, gra_enforce_affinity)]],
         {ns = nsid, resource = name, node = nodeid,
          rel = opts.rel, description = opts.description, enforce_affinity = opts.enforce_affinity,
          metadata = json.encode(metadata) }
      )
      if err ~= nil then
         error('could not assign resource ' .. name .. ' in namespace ' .. ns .. ' to node ' .. node .. ': ' ..
               err)
      end
   end
end
------------------------------------------
-- Resource claims                      --
------------------------------------------

function clusterd.claim_resource(ns, name, pid)
   assert(ns ~= nil, 'namespace must be provided to claim resource')
   assert(name ~= nil, 'name must be provided to claim resource')
   assert(pid ~= nil, 'process id must be provided to claim resource')

   resource = clusterd.get_global_resource(ns, name)
   if resource == nil then
      assert('resource ' .. name .. ' in namespace ' .. ns .. ' not found')
   end

   proc = clusterd.get_process(resource.ns, pid)
   if proc == nil then
      assert('process ' .. pid .. ' in namespace ' .. ns .. ' not found')
   end

   -- If this resource has any assignment that limit it to a certain
   -- node, then make sure this process is assigned to that
   -- node. Otherwise, disallow the operation

   nodes, err = api.run(
      [[SELECT DISTINCT gra_node FROM global_resource_assignment
        WHERE gra_ns=$ns AND gra_resource=$name AND gra_enforce_affinity]],
      { ns=resource.ns, name=resource.name }
   )
   if err ~= nil then
      error('could not get resource assignments: ' .. err)
   end

   if #nodes > 1 then
      error('this resource is assigned to many nodes with different affinities')
   end

   if proc.ps_placement ~= nodes[1].gra_node then
      error('process ' .. proc.ps_id .. ' cannot claim this resource. ' ..
               'The resource requires processes to be on node ' .. nodes[1].gra_node .. ', ' ..
               'but the process is placed on ' .. proc.ps_placement)
   end

   _, err = api.run(
      [[REPLACE INTO global_resource_claim(grc_ns, grc_resource, grc_process)
        VALUES ($ns, $name, $proc)]],
      { ns = resource.ns, name = resource.name, proc = proc }
   )
   if err ~= nil then
      error('could not claim resource: ' .. err)
   end
end

function clusterd.release_claim(ns, name, pid)
   assert(ns ~= nil, 'namespace must be provided to release resource claim')
   assert(name ~= nil, 'name must be provided to release resource claim')

   resource = clusterd.get_global_resource(ns, name)
   if resource == nil then
      return
   end

   proc = clusterd.resolve_process(resource.ns, pid)
   if proc == nil then
      return
   end

   _, err = api.run(
      [[DELETE FROM global_resource_claim WHERE grc_ns=$ns AND
        grc_resource=$name AND grc_process=$proc]],
      { ns = resource.ns, name = resource.name, proc = proc }
   )
   if err ~= nil then
      error('could not release resource claim: ' .. err)
   end
end

return clusterd

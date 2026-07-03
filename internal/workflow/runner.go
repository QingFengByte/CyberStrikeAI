package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"cyberstrike-ai/internal/agent"
	"cyberstrike-ai/internal/config"
	"cyberstrike-ai/internal/database"
	"cyberstrike-ai/internal/multiagent"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type RunArgs struct {
	DB                 *database.DB
	Logger             *zap.Logger
	Role               config.RoleConfig
	AppCfg             *config.Config
	Agent              *agent.Agent
	ConversationID     string
	ProjectID          string
	UserMessage        string
	History            []agent.ChatMessage
	RoleTools          []string
	AgentsMarkdownDir  string
	SystemPromptExtra  string
	AssistantMessageID string
	Progress           agent.ProgressCallback
}

type RunResult struct {
	Response string
	RunID    string
}

type graphDef struct {
	Nodes  []graphNode    `json:"nodes"`
	Edges  []graphEdge    `json:"edges"`
	Config map[string]any `json:"config"`
}

type graphNode struct {
	ID       string         `json:"id"`
	Type     string         `json:"type"`
	Label    string         `json:"label"`
	Position graphPosition  `json:"position"`
	Config   map[string]any `json:"config"`
}

type graphEdge struct {
	ID     string         `json:"id"`
	Source string         `json:"source"`
	Target string         `json:"target"`
	Label  string         `json:"label"`
	Config map[string]any `json:"config"`
}

type graphPosition struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type workflowExecState struct {
	inputs      map[string]any
	outputs     map[string]any
	nodeOutputs map[string]map[string]any
	lastOutput  map[string]any
	executed    []string
	skipped     []string
	workflowRunID string
	// 图编排内多个 Agent 节点各自从第 1 轮上报 iteration；累计偏移避免对话页迭代序号回跳与流式条目复用错乱。
	mainIterationOffset int
	segmentMaxIteration int
}

// ShouldAutoRunRoleWorkflow returns true when a role explicitly binds a workflow
// and does not turn it off. Empty policy defaults to auto to keep role UX simple.
func ShouldAutoRunRoleWorkflow(role config.RoleConfig) bool {
	if strings.TrimSpace(role.WorkflowID) == "" {
		return false
	}
	policy := strings.ToLower(strings.TrimSpace(role.WorkflowPolicy))
	return policy == "" || policy == "auto"
}

// RunRoleBoundWorkflow executes the persisted role-bound workflow graph.
// Control nodes are interpreted locally, tool nodes call the existing MCP bridge,
// and agent nodes reuse the existing Eino ADK runners so role-bound flows share
// the same model/tool/session behavior as the chat page.
func RunRoleBoundWorkflow(ctx context.Context, args RunArgs) (*RunResult, error) {
	if args.DB == nil {
		return nil, fmt.Errorf("workflow db is nil")
	}
	workflowID := strings.TrimSpace(args.Role.WorkflowID)
	if workflowID == "" {
		return nil, fmt.Errorf("角色未绑定工作流")
	}
	wf, err := args.DB.GetWorkflowDefinition(workflowID)
	if err != nil {
		return nil, err
	}
	if wf == nil {
		return nil, fmt.Errorf("角色绑定的工作流不存在: %s", workflowID)
	}
	if !wf.Enabled {
		return nil, fmt.Errorf("角色绑定的工作流已禁用: %s", workflowID)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	runID := uuid.NewString()
	input := map[string]interface{}{
		"message":         args.UserMessage,
		"conversationId":  args.ConversationID,
		"projectId":       args.ProjectID,
		"role":            args.Role.Name,
		"workflowId":      wf.ID,
		"workflowVersion": wf.Version,
	}
	inputJSON, _ := json.Marshal(input)
	run := &database.WorkflowRun{
		ID:              runID,
		WorkflowID:      wf.ID,
		WorkflowVersion: wf.Version,
		ConversationID:  args.ConversationID,
		ProjectID:       args.ProjectID,
		RoleID:          args.Role.Name,
		Status:          "running",
		InputJSON:       string(inputJSON),
		StartedAt:       time.Now(),
	}
	if err := args.DB.CreateWorkflowRun(run); err != nil {
		return nil, err
	}
	if args.Progress != nil {
		args.Progress("workflow_start", fmt.Sprintf("开始运行流程「%s」", wf.Name), map[string]interface{}{
			"workflowId":      wf.ID,
			"workflowName":    wf.Name,
			"workflowVersion": wf.Version,
			"workflowRunId":   runID,
			"conversationId":  args.ConversationID,
		})
	}

	graph, err := parseGraph(wf.GraphJSON)
	if err != nil {
		_ = args.DB.FinishWorkflowRun(runID, "failed", "", err.Error())
		return nil, err
	}
	state := &workflowExecState{
		inputs:        input,
		outputs:       make(map[string]any),
		nodeOutputs:   make(map[string]map[string]any),
		workflowRunID: runID,
	}
	if err := executeGraph(ctx, args, runID, graph, state); err != nil {
		_ = args.DB.FinishWorkflowRun(runID, "failed", "", err.Error())
		return nil, err
	}

	output := map[string]interface{}{
		"workflowId":      wf.ID,
		"workflowName":    wf.Name,
		"workflowVersion": wf.Version,
		"workflowRunId":   runID,
		"status":          "completed",
		"outputs":         state.outputs,
		"executedNodes":   state.executed,
		"skippedNodes":    state.skipped,
	}
	outputJSON, _ := json.Marshal(output)

	response := renderWorkflowResponse(args.Role.Name, wf.Name, wf.Version, runID, state)
	if err := args.DB.FinishWorkflowRun(runID, "completed", string(outputJSON), ""); err != nil {
		return nil, err
	}
	if args.Progress != nil {
		args.Progress("workflow_done", fmt.Sprintf("流程「%s」运行完成", wf.Name), map[string]interface{}{
			"workflowRunId": runID,
			"workflowId":    wf.ID,
			"outputs":       state.outputs,
			"response":      response,
		})
	}
	if args.Logger != nil {
		args.Logger.Info("role-bound workflow completed",
			zap.String("workflow_id", wf.ID),
			zap.String("workflow_run_id", runID),
			zap.String("conversation_id", args.ConversationID),
			zap.String("role", args.Role.Name),
		)
	}
	return &RunResult{Response: response, RunID: runID}, nil
}

func parseGraph(raw string) (*graphDef, error) {
	var g graphDef
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &g); err != nil {
		return nil, fmt.Errorf("解析工作流图失败: %w", err)
	}
	if len(g.Nodes) == 0 {
		return nil, fmt.Errorf("工作流没有节点")
	}
	if g.Config == nil {
		g.Config = make(map[string]any)
	}
	return &g, nil
}

func executeGraph(ctx context.Context, args RunArgs, runID string, g *graphDef, state *workflowExecState) error {
	nodes := make(map[string]graphNode, len(g.Nodes))
	inDegree := make(map[string]int, len(g.Nodes))
	outgoing := make(map[string][]graphEdge)
	for _, node := range g.Nodes {
		node.ID = strings.TrimSpace(node.ID)
		if node.ID == "" {
			continue
		}
		if strings.TrimSpace(node.Type) == "" {
			node.Type = "tool"
		}
		if node.Config == nil {
			node.Config = make(map[string]any)
		}
		nodes[node.ID] = node
		inDegree[node.ID] = 0
	}
	for _, edge := range g.Edges {
		if _, ok := nodes[edge.Source]; !ok {
			continue
		}
		if _, ok := nodes[edge.Target]; !ok {
			continue
		}
		outgoing[edge.Source] = append(outgoing[edge.Source], edge)
		inDegree[edge.Target]++
	}
	for source := range outgoing {
		sort.SliceStable(outgoing[source], func(i, j int) bool {
			a := nodes[outgoing[source][i].Target]
			b := nodes[outgoing[source][j].Target]
			if a.Position.Y != b.Position.Y {
				return a.Position.Y < b.Position.Y
			}
			if a.Position.X != b.Position.X {
				return a.Position.X < b.Position.X
			}
			return outgoing[source][i].Target < outgoing[source][j].Target
		})
	}

	var queue []string
	for id, node := range nodes {
		if strings.EqualFold(node.Type, "start") {
			queue = append(queue, id)
		}
	}
	if len(queue) == 0 {
		for id, deg := range inDegree {
			if deg == 0 {
				queue = append(queue, id)
			}
		}
	}
	sortNodeIDsByCanvas(queue, nodes)
	seen := make(map[string]bool)
	remainingIncoming := make(map[string]int, len(inDegree))
	for id, deg := range inDegree {
		remainingIncoming[id] = deg
	}
	for len(queue) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		node := nodes[id]
		result, proceed, err := executeNode(ctx, args, runID, node, state)
		if err != nil {
			return err
		}
		state.nodeOutputs[id] = result
		state.lastOutput = result
		if proceed {
			edges := outgoing[id]
			if strings.EqualFold(node.Type, "condition") {
				emitConditionBranchProgress(args, runID, node, edges, nodes, state)
			}
			for edgeIdx, edge := range edges {
				if !edgeAllowed(edge, node, edgeIdx, state) {
					continue
				}
				remainingIncoming[edge.Target]--
				if remainingIncoming[edge.Target] > 0 {
					continue
				}
				queue = append(queue, edge.Target)
			}
			sortNodeIDsByCanvas(queue, nodes)
		}
	}
	return nil
}

func sortNodeIDsByCanvas(ids []string, nodes map[string]graphNode) {
	sort.SliceStable(ids, func(i, j int) bool {
		a := nodes[ids[i]]
		b := nodes[ids[j]]
		if a.Position.Y != b.Position.Y {
			return a.Position.Y < b.Position.Y
		}
		if a.Position.X != b.Position.X {
			return a.Position.X < b.Position.X
		}
		return ids[i] < ids[j]
	})
}

func executeNode(ctx context.Context, args RunArgs, runID string, node graphNode, state *workflowExecState) (map[string]any, bool, error) {
	label := node.Label
	if strings.TrimSpace(label) == "" {
		label = node.ID
	}
	nodeRunID := uuid.NewString()
	input := map[string]any{
		"nodeId":   node.ID,
		"nodeType": node.Type,
		"label":    label,
		"inputs":   state.inputs,
		"previous": state.lastOutput,
	}
	inputJSON, _ := json.Marshal(input)
	if err := args.DB.CreateWorkflowNodeRun(&database.WorkflowNodeRun{
		ID:        nodeRunID,
		RunID:     runID,
		NodeID:    node.ID,
		Status:    "running",
		InputJSON: string(inputJSON),
		StartedAt: time.Now(),
	}); err != nil {
		return nil, false, err
	}
	if args.Progress != nil {
		args.Progress("workflow_node_start", fmt.Sprintf("开始节点：%s", label), map[string]any{
			"workflowRunId": runID,
			"nodeRunId":     nodeRunID,
			"nodeId":        node.ID,
			"nodeType":      node.Type,
			"label":         label,
		})
	}

	result, proceed, status, errText := runBuiltinNode(ctx, args, node, state)
	outputJSON, _ := json.Marshal(result)
	if err := args.DB.FinishWorkflowNodeRun(nodeRunID, status, string(outputJSON), errText); err != nil {
		return nil, false, err
	}
	if status == "skipped" {
		state.skipped = append(state.skipped, label)
	} else {
		state.executed = append(state.executed, label)
	}
	if args.Progress != nil {
		progressData := map[string]any{
			"workflowRunId": runID,
			"nodeRunId":     nodeRunID,
			"nodeId":        node.ID,
			"nodeType":      node.Type,
			"label":         label,
			"status":        status,
			"output":        result,
		}
		progressMsg := fmt.Sprintf("节点完成：%s（%s）", label, status)
		if strings.EqualFold(node.Type, "condition") {
			matched := false
			if v, ok := result["matched"].(bool); ok {
				matched = v
			}
			expr := cfgString(node.Config, "expression")
			if matched {
				progressMsg = fmt.Sprintf("条件判断：%s → 是", label)
			} else {
				progressMsg = fmt.Sprintf("条件判断：%s → 否", label)
			}
			progressData["expression"] = expr
			progressData["matched"] = matched
		}
		args.Progress("workflow_node_result", progressMsg, progressData)
	}
	return result, proceed, nil
}

func runBuiltinNode(ctx context.Context, args RunArgs, node graphNode, state *workflowExecState) (map[string]any, bool, string, string) {
	cfg := node.Config
	switch strings.ToLower(strings.TrimSpace(node.Type)) {
	case "start":
		out := map[string]any{
			"output":         state.inputs["message"],
			"message":        state.inputs["message"],
			"conversationId": state.inputs["conversationId"],
			"projectId":      state.inputs["projectId"],
		}
		return out, true, "completed", ""
	case "condition":
		expr := cfgString(cfg, "expression")
		ok := evalCondition(expr, state)
		out := map[string]any{"output": ok, "condition": expr, "matched": ok}
		// 条件节点始终继续，由出边条件（或连线标签/顺序）决定走「是/否」分支。
		return out, true, "completed", ""
	case "output":
		key := cfgString(cfg, "output_key")
		if key == "" {
			key = "result"
		}
		value := resolveTemplate(cfgString(cfg, "source"), state)
		state.outputs[key] = value
		return map[string]any{"output": value, "outputs": map[string]any{key: value}}, true, "completed", ""
	case "end":
		value := resolveTemplate(cfgString(cfg, "result_template"), state)
		return map[string]any{"output": value}, false, "completed", ""
	case "tool":
		return runToolNode(ctx, args, node, state)
	case "agent":
		return runAgentNode(ctx, args, node, state)
	case "hitl":
		return runHITLNode(args, node, state)
	default:
		reason := "未知节点类型"
		return map[string]any{"output": "", "skipped": true, "reason": reason, "node_type": node.Type}, true, "skipped", reason
	}
}

func runToolNode(ctx context.Context, args RunArgs, node graphNode, state *workflowExecState) (map[string]any, bool, string, string) {
	toolName := cfgString(node.Config, "tool_name")
	if toolName == "" {
		errText := "工具节点未选择 MCP 工具"
		return map[string]any{"output": "", "error": errText}, false, "failed", errText
	}
	if args.Agent == nil {
		errText := "工具节点执行失败：Agent 为空"
		return map[string]any{"output": "", "tool_name": toolName, "error": errText}, false, "failed", errText
	}
	toolArgs, err := parseToolArguments(cfgString(node.Config, "arguments"), state)
	if err != nil {
		errText := fmt.Sprintf("工具参数不是合法 JSON：%v", err)
		return map[string]any{"output": "", "tool_name": toolName, "error": errText}, false, "failed", errText
	}
	if args.Progress != nil {
		args.Progress("workflow_tool_start", fmt.Sprintf("调用工具：%s", toolName), map[string]any{
			"nodeId": node.ID,
			"tool":   toolName,
			"args":   toolArgs,
		})
	}
	result, err := args.Agent.ExecuteMCPToolForConversation(ctx, args.ConversationID, toolName, toolArgs)
	if err != nil {
		errText := err.Error()
		return map[string]any{"output": "", "tool_name": toolName, "arguments": toolArgs, "error": errText}, false, "failed", errText
	}
	output := ""
	executionID := ""
	isError := false
	if result != nil {
		output = result.Result
		executionID = result.ExecutionID
		isError = result.IsError
	}
	out := map[string]any{
		"output":       output,
		"tool_name":    toolName,
		"arguments":    toolArgs,
		"execution_id": executionID,
		"is_error":     isError,
	}
	if key := cfgString(node.Config, "output_key"); key != "" {
		state.outputs[key] = output
	}
	if isError {
		errText := strings.TrimSpace(output)
		if errText == "" {
			errText = "工具返回错误"
		}
		return out, false, "failed", errText
	}
	return out, true, "completed", ""
}

func runAgentNode(ctx context.Context, args RunArgs, node graphNode, state *workflowExecState) (map[string]any, bool, string, string) {
	if args.AppCfg == nil || args.Agent == nil {
		errText := "Agent 节点执行失败：应用配置或 Agent 为空"
		return map[string]any{"output": "", "error": errText}, false, "failed", errText
	}
	mode := strings.ToLower(cfgString(node.Config, "agent_mode"))
	if mode == "" {
		mode = "eino_single"
	}
	inputSource := cfgString(node.Config, "input_source")
	if inputSource == "" {
		inputSource = "{{previous.output}}"
	}
	upstreamInput := strings.TrimSpace(resolveTemplate(inputSource, state))
	message := buildAgentNodeMessage(node, state)
	var result *multiagent.RunResult
	var err error
	state.segmentMaxIteration = 0
	agentProgress := workflowAgentProgress(args.Progress, state, node)
	switch mode {
	case "eino_single", "single", "chat":
		result, err = multiagent.RunEinoSingleChatModelAgent(
			ctx,
			args.AppCfg,
			&args.AppCfg.MultiAgent,
			args.Agent,
			args.DB,
			args.Logger,
			args.ConversationID,
			args.ProjectID,
			message,
			args.History,
			args.RoleTools,
			agentProgress,
			nil,
			args.SystemPromptExtra,
		)
	default:
		result, err = multiagent.RunDeepAgent(
			ctx,
			args.AppCfg,
			&args.AppCfg.MultiAgent,
			args.Agent,
			args.DB,
			args.Logger,
			args.ConversationID,
			args.ProjectID,
			message,
			args.History,
			args.RoleTools,
			agentProgress,
			args.AgentsMarkdownDir,
			mode,
			nil,
			args.SystemPromptExtra,
		)
	}
	if err != nil {
		errText := err.Error()
		state.mainIterationOffset += state.segmentMaxIteration
		return map[string]any{"output": "", "mode": mode, "error": errText}, false, "failed", errText
	}
	state.mainIterationOffset += state.segmentMaxIteration
	response := ""
	mcpIDs := []string{}
	if result != nil {
		response = result.Response
		mcpIDs = result.MCPExecutionIDs
	}
	if args.Progress != nil {
		args.Progress("workflow_agent_output", response, map[string]any{
			"nodeId":          node.ID,
			"label":           firstNonEmpty(node.Label, node.ID),
			"mode":            mode,
			"inputSource":     inputSource,
			"inputPreview":    truncateWorkflowPreview(upstreamInput, 500),
			"mcpExecutionIds": mcpIDs,
		})
	}
	if key := cfgString(node.Config, "output_key"); key != "" {
		state.outputs[key] = response
	}
	return map[string]any{
		"output":            response,
		"mode":              mode,
		"mcp_execution_ids": mcpIDs,
	}, true, "completed", ""
}

func buildAgentNodeMessage(node graphNode, state *workflowExecState) string {
	instruction := strings.TrimSpace(resolveTemplate(cfgString(node.Config, "instruction"), state))
	inputSource := cfgString(node.Config, "input_source")
	if inputSource == "" {
		inputSource = "{{previous.output}}"
	}
	upstreamInput := strings.TrimSpace(resolveTemplate(inputSource, state))
	if instruction == "" {
		if upstreamInput != "" {
			return fmt.Sprintf("请基于上游节点输出继续处理：\n%s", upstreamInput)
		}
		return fmt.Sprintf("请基于上游节点输出继续处理：\n%v", state.lastOutput["output"])
	}
	if upstreamInput == "" {
		return instruction
	}
	return strings.TrimSpace(fmt.Sprintf("上游输入：\n%s\n\n节点指令：\n%s", upstreamInput, instruction))
}

func workflowAgentProgress(progress agent.ProgressCallback, state *workflowExecState, node graphNode) agent.ProgressCallback {
	if progress == nil {
		return nil
	}
	return func(eventType, message string, data interface{}) {
		switch eventType {
		case "response_start", "response_delta", "response", "done":
			return
		default:
			enrichWorkflowAgentEventData(data, state, node)
			if eventType == "iteration" {
				applyWorkflowMainIterationOffset(data, state)
			}
			progress(eventType, message, data)
		}
	}
}

func enrichWorkflowAgentEventData(data interface{}, state *workflowExecState, node graphNode) {
	m, ok := data.(map[string]interface{})
	if !ok || m == nil {
		return
	}
	if node.ID != "" {
		m["workflowNodeId"] = node.ID
	}
	if state != nil && strings.TrimSpace(state.workflowRunID) != "" {
		m["workflowRunId"] = state.workflowRunID
	}
}

func applyWorkflowMainIterationOffset(data interface{}, state *workflowExecState) {
	if state == nil {
		return
	}
	m, ok := data.(map[string]interface{})
	if !ok || m == nil {
		return
	}
	scope, _ := m["einoScope"].(string)
	if strings.TrimSpace(scope) != "main" {
		return
	}
	raw := iterationNumberFromProgressData(m)
	if raw <= 0 {
		return
	}
	if raw > state.segmentMaxIteration {
		state.segmentMaxIteration = raw
	}
	m["iteration"] = raw + state.mainIterationOffset
}

func iterationNumberFromProgressData(m map[string]interface{}) int {
	switch v := m["iteration"].(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	case float32:
		return int(v)
	default:
		return 0
	}
}

func runHITLNode(args RunArgs, node graphNode, state *workflowExecState) (map[string]any, bool, string, string) {
	prompt := resolveTemplate(cfgString(node.Config, "prompt"), state)
	reviewer := cfgString(node.Config, "reviewer")
	if args.Progress != nil {
		args.Progress("workflow_hitl_checkpoint", "人工确认节点已记录", map[string]any{
			"nodeId":   node.ID,
			"prompt":   prompt,
			"reviewer": reviewer,
			"mode":     "record_only",
		})
	}
	return map[string]any{
		"output":   prompt,
		"prompt":   prompt,
		"reviewer": reviewer,
		"approved": true,
		"mode":     "record_only",
	}, true, "completed", ""
}

func parseToolArguments(raw string, state *workflowExecState) (map[string]interface{}, error) {
	if raw == "" {
		return map[string]interface{}{}, nil
	}
	raw = strings.TrimSpace(resolveTemplate(raw, state))
	if raw == "" {
		return map[string]interface{}{}, nil
	}
	var args map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		return nil, err
	}
	if args == nil {
		args = map[string]interface{}{}
	}
	return args, nil
}

func edgeAllowed(edge graphEdge, sourceNode graphNode, edgeIndex int, state *workflowExecState) bool {
	cond := firstNonEmpty(cfgString(edge.Config, "condition"), cfgString(edge.Config, "expression"))
	if cond != "" {
		return evalCondition(cond, state)
	}
	if strings.EqualFold(strings.TrimSpace(sourceNode.Type), "condition") {
		return conditionBranchAllowed(edge, edgeIndex, state)
	}
	return true
}

func conditionBranchAllowed(edge graphEdge, edgeIndex int, state *workflowExecState) bool {
	matched := conditionMatched(state)
	if branch := conditionBranchHint(edge); branch != "" {
		return (branch == "true" && matched) || (branch == "false" && !matched)
	}
	switch edgeIndex {
	case 0:
		return matched
	case 1:
		return !matched
	default:
		return false
	}
}

func conditionMatched(state *workflowExecState) bool {
	v := strings.ToLower(cleanComparable(fmt.Sprint(valueFromPath("previous.matched", state))))
	return v == "true" || v == "1"
}

func conditionBranchHint(edge graphEdge) string {
	if edge.Config != nil {
		switch strings.ToLower(strings.TrimSpace(cfgString(edge.Config, "branch"))) {
		case "true", "yes", "y", "是":
			return "true"
		case "false", "no", "n", "否":
			return "false"
		}
	}
	switch strings.ToLower(strings.TrimSpace(edge.Label)) {
	case "true", "yes", "y", "是":
		return "true"
	case "false", "no", "n", "否":
		return "false"
	}
	return ""
}

func emitConditionBranchProgress(args RunArgs, runID string, node graphNode, edges []graphEdge, nodes map[string]graphNode, state *workflowExecState) {
	if args.Progress == nil || len(edges) == 0 {
		return
	}
	for edgeIdx, edge := range edges {
		allowed := edgeAllowed(edge, node, edgeIdx, state)
		target := nodes[edge.Target]
		targetLabel := strings.TrimSpace(target.Label)
		if targetLabel == "" {
			targetLabel = edge.Target
		}
		branchLabel := strings.TrimSpace(edge.Label)
		if branchLabel == "" {
			switch edgeIdx {
			case 0:
				branchLabel = "是"
			case 1:
				branchLabel = "否"
			default:
				branchLabel = fmt.Sprintf("分支 %d", edgeIdx+1)
			}
		}
		cond := firstNonEmpty(cfgString(edge.Config, "condition"), cfgString(edge.Config, "expression"))
		eventType := "workflow_branch_skipped"
		msg := fmt.Sprintf("跳过分支「%s」→ %s", branchLabel, targetLabel)
		if allowed {
			eventType = "workflow_branch_taken"
			msg = fmt.Sprintf("执行分支「%s」→ %s", branchLabel, targetLabel)
		}
		args.Progress(eventType, msg, map[string]any{
			"workflowRunId": runID,
			"nodeId":        node.ID,
			"nodeType":      node.Type,
			"label":         node.Label,
			"branchLabel":   branchLabel,
			"targetId":      edge.Target,
			"targetLabel":   targetLabel,
			"edgeCondition": cond,
			"matched":       conditionMatched(state),
		})
	}
}

func cfgString(cfg map[string]any, key string) string {
	if cfg == nil {
		return ""
	}
	if v, ok := cfg[key]; ok {
		return strings.TrimSpace(fmt.Sprint(v))
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if s := strings.TrimSpace(value); s != "" {
			return s
		}
	}
	return ""
}

func truncateWorkflowPreview(s string, limit int) string {
	s = strings.TrimSpace(s)
	if limit <= 0 || len([]rune(s)) <= limit {
		return s
	}
	runes := []rune(s)
	return string(runes[:limit]) + "..."
}

var templateVarRe = regexp.MustCompile(`\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}`)

func resolveTemplate(s string, state *workflowExecState) string {
	if strings.TrimSpace(s) == "" {
		return fmt.Sprint(valueFromPath("previous.output", state))
	}
	return templateVarRe.ReplaceAllStringFunc(s, func(match string) string {
		m := templateVarRe.FindStringSubmatch(match)
		if len(m) != 2 {
			return match
		}
		return fmt.Sprint(valueFromPath(m[1], state))
	})
}

func valueFromPath(path string, state *workflowExecState) any {
	parts := strings.Split(path, ".")
	if len(parts) == 0 {
		return ""
	}
	var cur any
	switch parts[0] {
	case "inputs", "input":
		cur = state.inputs
	case "previous", "prev":
		cur = state.lastOutput
	case "outputs":
		cur = state.outputs
	default:
		if v, ok := state.inputs[parts[0]]; ok {
			cur = v
		} else if v, ok := state.nodeOutputs[parts[0]]; ok {
			cur = v
		} else {
			return ""
		}
	}
	for _, p := range parts[1:] {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[p]
	}
	if cur == nil {
		return ""
	}
	return cur
}

func evalCondition(expr string, state *workflowExecState) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return true
	}
	resolved := strings.TrimSpace(resolveTemplate(expr, state))
	switch {
	case strings.Contains(resolved, "!="):
		parts := strings.SplitN(resolved, "!=", 2)
		return cleanComparable(parts[0]) != cleanComparable(parts[1])
	case strings.Contains(resolved, "=="):
		parts := strings.SplitN(resolved, "==", 2)
		return cleanComparable(parts[0]) == cleanComparable(parts[1])
	default:
		v := strings.ToLower(cleanComparable(resolved))
		return v != "" && v != "false" && v != "0" && v != "null"
	}
}

func cleanComparable(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, `"'`)
	return s
}

func renderWorkflowResponse(roleName, workflowName string, version int, runID string, state *workflowExecState) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("角色「%s」已完成工作流「%s」（版本 %d）。\n\n", roleName, workflowName, version))
	sb.WriteString(fmt.Sprintf("运行 ID：%s\n", runID))
	sb.WriteString(fmt.Sprintf("已执行节点：%d", len(state.executed)))
	if len(state.skipped) > 0 {
		sb.WriteString(fmt.Sprintf("，跳过节点：%d", len(state.skipped)))
	}
	sb.WriteString("\n\n")
	if len(state.outputs) > 0 {
		sb.WriteString("输出：\n")
		keys := make([]string, 0, len(state.outputs))
		for k := range state.outputs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			sb.WriteString(fmt.Sprintf("- %s：%v\n", k, state.outputs[k]))
		}
	} else {
		sb.WriteString("暂无输出。请检查是否配置了输出节点，或条件分支是否命中。\n")
	}
	if len(state.skipped) > 0 {
		sb.WriteString("\n未执行的节点类型仍会保留运行记录：")
		sb.WriteString(strings.Join(state.skipped, "、"))
		sb.WriteString("。")
	}
	return strings.TrimSpace(sb.String())
}

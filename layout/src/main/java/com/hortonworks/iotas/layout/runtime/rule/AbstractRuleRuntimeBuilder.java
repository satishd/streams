package com.hortonworks.iotas.layout.runtime.rule;

import com.hortonworks.iotas.layout.design.pipeline.StageAction;
import com.hortonworks.iotas.layout.design.rule.Rule;
import com.hortonworks.iotas.layout.design.rule.action.Action;
import com.hortonworks.iotas.layout.design.rule.action.NotifierAction;
import com.hortonworks.iotas.layout.runtime.ActionRuntime;
import com.hortonworks.iotas.layout.runtime.TransformActionRuntime;
import com.hortonworks.iotas.layout.design.pipeline.JoinAction;
import com.hortonworks.iotas.layout.runtime.pipeline.JoinActionRuntime;
import com.hortonworks.iotas.layout.design.pipeline.SplitAction;
import com.hortonworks.iotas.layout.runtime.pipeline.SplitActionRuntime;
import com.hortonworks.iotas.layout.runtime.pipeline.StageActionRuntime;
import com.hortonworks.iotas.layout.runtime.transform.AddHeaderTransformRuntime;
import com.hortonworks.iotas.layout.runtime.transform.IdentityTransformRuntime;
import com.hortonworks.iotas.layout.runtime.transform.MergeTransformRuntime;
import com.hortonworks.iotas.layout.runtime.transform.ProjectionTransformRuntime;
import com.hortonworks.iotas.layout.runtime.transform.SubstituteTransformRuntime;
import com.hortonworks.iotas.layout.runtime.transform.TransformRuntime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractRuleRuntimeBuilder implements RuleRuntimeBuilder {
    protected List<ActionRuntime> actions;

    @Override
    public void buildActions() {
        List<ActionRuntime> runtimeActions = new ArrayList<>();
        Rule rule = getRule();
        for (Action action : rule.getActions()) {
            final ActionRuntime actionRuntime = createActionRuntime(rule, action);
            runtimeActions.add(actionRuntime);
        }
        actions = runtimeActions;
    }

    protected ActionRuntime createActionRuntime(Rule rule, Action action) {
        ActionRuntime actionRuntime = null;
        //todo create ActionRuntimeFactory to get respective runtime instances.
        if(action instanceof NotifierAction) {
            String streamId = rule.getRuleProcessorName() + "." + rule.getName() + "."
                    + rule.getId() + "." + action.getName();
            /*
             * Add a TransformAction to perform necessary transformation for notification
             */
            actionRuntime = new TransformActionRuntime(streamId, getNotificationTransforms((NotifierAction) action, getRule().getId()));
        } else if(action instanceof SplitAction){
            actionRuntime = new SplitActionRuntime((SplitAction) action);
        } else if(action instanceof JoinAction){
            actionRuntime = new JoinActionRuntime((JoinAction) action);
        } else if(action instanceof StageAction){
            actionRuntime = new StageActionRuntime((StageAction) action);
        } else {
            throw new IllegalArgumentException("Action: "+action+" is not supported");
        }
        return actionRuntime;
    }

    /**
     * Returns the necessary transforms to perform based on the action.
     */
    private static List<TransformRuntime> getNotificationTransforms(NotifierAction action, Long ruleId) {
        List<TransformRuntime> transformRuntimes = new ArrayList<>();
        if (action.getOutputFieldsAndDefaults() != null && !action.getOutputFieldsAndDefaults().isEmpty()) {
            transformRuntimes.add(new MergeTransformRuntime(action.getOutputFieldsAndDefaults()));
            transformRuntimes.add(new SubstituteTransformRuntime(action.getOutputFieldsAndDefaults().keySet()));
            transformRuntimes.add(new ProjectionTransformRuntime(action.getOutputFieldsAndDefaults().keySet()));
        }

        if (action.isIncludeMeta()) {
            Map<String, Object> headers = new HashMap<>();
            headers.put(AddHeaderTransformRuntime.HEADER_FIELD_NOTIFIER_NAME, action.getNotifierName());
            headers.put(AddHeaderTransformRuntime.HEADER_FIELD_RULE_ID, ruleId);
            transformRuntimes.add(new AddHeaderTransformRuntime(headers));
        }

        // default is to just forward the event
        if(transformRuntimes.isEmpty()) {
            transformRuntimes.add(new IdentityTransformRuntime());
        }
        return transformRuntimes;
    }

    protected abstract Rule getRule();
}

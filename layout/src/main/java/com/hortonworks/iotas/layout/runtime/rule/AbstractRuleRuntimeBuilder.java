package com.hortonworks.iotas.layout.runtime.rule;

import com.hortonworks.iotas.layout.design.rule.Rule;
import com.hortonworks.iotas.layout.design.rule.action.Action;
import com.hortonworks.iotas.layout.design.rule.action.NotifierAction;
import com.hortonworks.iotas.layout.runtime.ActionRuntime;
import com.hortonworks.iotas.layout.runtime.TransformActionRuntime;
import com.hortonworks.iotas.layout.runtime.pipeline.SplitAction;
import com.hortonworks.iotas.layout.runtime.pipeline.SplitActionRuntime;
import com.hortonworks.iotas.layout.runtime.transform.AddHeaderTransform;
import com.hortonworks.iotas.layout.runtime.transform.IdentityTransform;
import com.hortonworks.iotas.layout.runtime.transform.MergeTransform;
import com.hortonworks.iotas.layout.runtime.transform.ProjectionTransform;
import com.hortonworks.iotas.layout.runtime.transform.SubstituteTransform;
import com.hortonworks.iotas.layout.runtime.transform.Transform;

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
        if(action instanceof NotifierAction) {
            String streamId = rule.getRuleProcessorName() + "." + rule.getName() + "."
                    + rule.getId() + "." + action.getName();
            /*
             * Add an TransformAction to perform necessary transformation for notification
             */
            actionRuntime = new TransformActionRuntime(streamId, getTransforms(action));
        } else if(action instanceof SplitAction){
            actionRuntime = new SplitActionRuntime((SplitAction) action);
        } else {
            throw new IllegalArgumentException("Action: "+action+" is not supported");
        }
        return actionRuntime;
    }

    protected List<Transform> getTransforms(Action action) {
        if(action instanceof NotifierAction) {
            return getNotificationTransforms((NotifierAction) action);
        } else {
            // split action will not have any transforms for now. SplitAction would split and send events without any
            // transformations. We can add them later
            return Collections.<Transform>singletonList(new IdentityTransform());
        }
    }

    /**
     * Returns the necessary transforms to perform based on the action.
     */
    private List<Transform> getNotificationTransforms(NotifierAction action) {
        List<Transform> transforms = new ArrayList<>();
        if (action.getOutputFieldsAndDefaults() != null && !action.getOutputFieldsAndDefaults().isEmpty()) {
            transforms.add(new MergeTransform(action.getOutputFieldsAndDefaults()));
            transforms.add(new SubstituteTransform(action.getOutputFieldsAndDefaults().keySet()));
            transforms.add(new ProjectionTransform(action.getOutputFieldsAndDefaults().keySet()));
        }

        if (action.isIncludeMeta()) {
            Map<String, Object> headers = new HashMap<>();
            headers.put(AddHeaderTransform.HEADER_FIELD_NOTIFIER_NAME, action.getNotifierName());
            headers.put(AddHeaderTransform.HEADER_FIELD_RULE_ID, getRule().getId());
            transforms.add(new AddHeaderTransform(headers));
        }

        // default is to just forward the event
        if(transforms.isEmpty()) {
            transforms.add(new IdentityTransform());
        }
        return transforms;
    }

    protected abstract Rule getRule();
}

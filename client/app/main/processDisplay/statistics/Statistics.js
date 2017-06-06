import {jsx, withSelector, Class, OnEvent, Scope, Text, createStateComponent, createReferenceComponent, updateOnlyWhenStateChanges} from 'view-utils';
import {StatisticChart} from './StatisticChart';
import {leaveGatewayAnalysisMode, getSelection} from '../views';
import {loadStatisticData, resetStatisticData, findSequenceFlowBetweenGatewayAndActivity} from './service';
import {DragHandle} from './DragHandle';
import {isInitial, isLoading} from 'utils';
import isEqual from 'lodash.isequal';

export const Statistics = withSelector(({getBpmnViewer}) => {
  return (parentNode, eventsBus) => {
    const State = createStateComponent();
    const Reference = createReferenceComponent();

    const template = <State>
      <div className="statisticsContainer">
        <Reference name="statisticsContainer" />
        <DragHandle />
        <Class className="open" selector="views" predicate={isSelectionComplete} />
        <button type="button" className="close">
          <OnEvent event="click" listener={leaveGatewayAnalysisMode} />
          <span>×</span>
        </button>
        <StatisticChart
          isLoading={isLoadingSomething}
          data={getChartData(relativeData)}
          chartConfig={{absoluteScale: false, onHoverChange}}>
          <Scope selector={getHeader(relativeHeader)}>
            Gateway:&nbsp;<Text property="gateway" />
            &nbsp;/
            EndEvent:&nbsp;<Text property="endEvent" />
            &nbsp;- Amount:&nbsp;
            <Text property="amount" />
          </Scope>
        </StatisticChart>
        <StatisticChart
          isLoading={isLoadingSomething}
          data={getChartData(absoluteData)}
          chartConfig={{absoluteScale: true, onHoverChange}}>
          <Scope selector={getHeader(absoluteHeader)}>
            Gateway:&nbsp;<Text property="gateway" />
            &nbsp;- Amount:&nbsp;
            <Text property="amount" />
          </Scope>
        </StatisticChart>
      </div>
    </State>;

    const templateUpdate = template(parentNode, eventsBus);

    return [
      templateUpdate,
      updateOnlyWhenStateChanges(({views, statistics: {correlation, height}}) => {
        const node = Reference.getNode('statisticsContainer');

        if (isSelectionComplete(views)) {
          node.style.height = (height || 350) + 'px';
        } else {
          node.style.height = '0px';
        }

        if (!isSelectionComplete(views) && !isInitial(correlation)) {
          resetStatisticData();
        }

        if (isSelectionComplete(views) && isInitial(correlation)) {
          loadStatisticData(getSelection(views));
        }
      }, areStatisticsStatesEqual)
    ];

    function areStatisticsStatesEqual(firstState, secondState) {
      if (!firstState || !secondState) {
        return false;
      }

      const firstSelection = getSelection(firstState.views);
      const secondSelection = getSelection(secondState.views);

      return isEqual(firstSelection, secondSelection) && isEqual(firstState.statistics, secondState.statistics);
    }

    function onHoverChange(hovered) {
      return (bar, index) => {
        const viewer = getBpmnViewer();
        const elementRegistry = viewer.get('elementRegistry');
        const canvas = viewer.get('canvas');

        elementRegistry.forEach((element) => {
          canvas.removeMarker(element, 'chart-hover');
        });

        if (hovered) {
          const {views, statistics:{correlation:{data:{followingNodes}}}} = State.getState();
          const gateway = getSelection(views).Gateway;
          const activity = Object.keys(followingNodes)[index];

          const sequenceFlow = findSequenceFlowBetweenGatewayAndActivity(elementRegistry, gateway, activity);

          canvas.addMarker(sequenceFlow, 'chart-hover');
        }
      };
    }
  };

  function isLoadingSomething({statistics: {correlation}}) {
    return isLoading(correlation);
  }

  function absoluteHeader({followingNodes}) {
    return Object.keys(followingNodes).reduce((prev, key) => {
      return prev + followingNodes[key].activityCount;
    }, 0);
  }

  function relativeHeader({total}) {
    return total;
  }

  function getHeader(amountFct) {
    return ({views, statistics: {correlation}}) => {
      const selection = getSelection(views);
      const gateway = selection && selection.Gateway;
      const endEvent = selection && selection.EndEvent;

      if (!correlation.data || !gateway || !endEvent) {
        return {};
      }

      const elementRegistry = getBpmnViewer().get('elementRegistry');

      return {
        gateway: elementRegistry.get(gateway).businessObject.name || gateway,
        endEvent: elementRegistry.get(endEvent).businessObject.name || endEvent,
        amount: amountFct(correlation.data)
      };
    };
  }

  function isSelectionComplete(views) {
    const selection = getSelection(views);

    return selection && selection.EndEvent && selection.Gateway;
  }

  function relativeData({activitiesReached, activityCount}) {
    return {
      value: activitiesReached / activityCount || 0,
      tooltip: (Math.round((activitiesReached / activityCount || 0) * 10000) / 100) + '% (' + activitiesReached + ' / ' + activityCount + ')'
    };
  }

  function absoluteData({activityCount}) {
    return {
      value: activityCount || 0,
      tooltip: activityCount || 0
    };
  }

  function getChartData(valueFct) {
    return ({statistics: {correlation}, views}) => {
      const selection = getSelection(views);
      const gateway = selection && selection.Gateway;

      if (!correlation.data || !gateway) {
        return [];
      }

      const elementRegistry = getBpmnViewer().get('elementRegistry');

      return Object.keys(correlation.data.followingNodes).map(key => {
        const data = correlation.data.followingNodes[key];

        const sequenceFlow = findSequenceFlowBetweenGatewayAndActivity(elementRegistry, gateway, key);

        return {
          ...valueFct(data),
          key: sequenceFlow.businessObject.name || elementRegistry.get(key).businessObject.name || key
        };
      });
    };
  }
});
